package com.gtm.vpointer

import android.content.Context
import android.net.ConnectivityManager
import android.net.Network
import android.net.NetworkRequest
import android.util.Log
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import java.io.InputStream
import java.io.OutputStream
import java.net.Inet4Address
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.NetworkInterface
import java.net.ServerSocket
import java.net.Socket
import java.util.Collections

/**
 * 纯应用层 TCP 端口转发，用于替代：
 *   socat TCP4-LISTEN:8000,bind=0.0.0.0,reuseaddr,fork \
 *         TCP4:192.168.73.1:80,so-bindtodevice=usb0
 *
 * SO_BINDTODEVICE 本身需要 root，非系统 App 不可用，因此改用等价的 Android 方式：
 *  - 通过 [ConnectivityManager] 找到 usb* 网卡对应的 [Network]，对上游 Socket 调用
 *    [Network.bindSocket]，强制其流量走该网络（等价于 so-bindtodevice）。
 *  - 若 usb 仅作为裸网卡存在、未被系统登记为 Network，则回退到把 Socket 的源地址
 *    绑定到该网卡的 IPv4 地址，Linux 依据源地址选择出口网卡。
 *
 * usb0 热插拔由 NetworkCallback + 2 秒轮询兜底感知：网卡在则转发，拔出则暂停上游
 * 连接（监听 socket 一直保留），重新插入自动恢复。
 */
class PortForwarder(
    private val context: Context,
    private val onStatus: (Status, String) -> Unit
) {
    enum class Status { STARTED, USB_UP, USB_DOWN, ERROR }

    companion object {
        const val TARGET_HOST = "192.168.73.1"
        const val TARGET_PORT = 80
        private const val TAG = "PortForwarder"
        private const val BUFFER = 8192
        private const val CONNECT_TIMEOUT_MS = 5000
        private const val RESCAN_INTERVAL_MS = 2000L
    }

    @Volatile private var server: ServerSocket? = null
    @Volatile private var running = false
    @Volatile private var listenPort = 0

    // usb 网卡检测结果
    @Volatile private var usbNetwork: Network? = null
    @Volatile private var usbInterfaceName: String? = null
    @Volatile private var usbLocalIp: InetAddress? = null

    private val connections = Collections.synchronizedSet(HashSet<Socket>())
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private var connMgr: ConnectivityManager? = null
    private var networkCallback: ConnectivityManager.NetworkCallback? = null

    val isRunning: Boolean get() = running

    /** 启动监听。端口绑定失败会回调 ERROR 并保持未运行状态。 */
    @Synchronized
    fun start(port: Int) {
        if (running) {
            Log.w(TAG, "start() ignored: already running on :$listenPort")
            return
        }
        val s = try {
            ServerSocket().apply {
                reuseAddress = true
                bind(InetSocketAddress(port)) // 0.0.0.0:port
            }
        } catch (e: Exception) {
            Log.e(TAG, "bind :$port failed", e)
            onStatus(Status.ERROR, "端口 $port 绑定失败: ${e.message}")
            return
        }
        server = s
        listenPort = port
        running = true

        connMgr = context.getSystemService(ConnectivityManager::class.java)
        registerUsbMonitor()

        onStatus(Status.STARTED, "转发已启动，监听 0.0.0.0:$port → $TARGET_HOST:$TARGET_PORT")
        rescanUsb(forceNotify = true) // 立即上报一次当前 usb 状态
        acceptLoop()
    }

    /** 停止监听并关闭所有连接。静默（不回调），由调用方负责上报 STOPPED。 */
    @Synchronized
    fun stop() {
        if (!running) return
        running = false
        unregisterUsbMonitor()
        server?.let { runCatching { it.close() } }
        server = null
        synchronized(connections) {
            connections.forEach { runCatching { it.close() } }
            connections.clear()
        }
        scope.cancel()
        listenPort = 0
        usbNetwork = null
        usbInterfaceName = null
        usbLocalIp = null
    }

    private fun acceptLoop() {
        val srv = server ?: return
        scope.launch {
            while (running) {
                val client = try {
                    srv.accept()
                } catch (e: Exception) {
                    if (running) Log.w(TAG, "accept failed: ${e.message}")
                    break
                }
                launch { handle(client) }
            }
        }
    }

    private suspend fun handle(client: Socket) {
        connections += client
        try {
            val upstream = connectUpstream() ?: return // usb 缺失或上游不可达：直接关闭本连接
            try {
                val cIn = client.getInputStream()
                val cOut = client.getOutputStream()
                val uIn = upstream.getInputStream()
                val uOut = upstream.getOutputStream()
                // 任一方向结束即收尾，关闭两端以解除对侧阻塞读
                val done = CompletableDeferred<Unit>()
                val a = scope.launch { runCatching { pump(cIn, uOut) }; done.complete(Unit) }
                val b = scope.launch { runCatching { pump(uIn, cOut) }; done.complete(Unit) }
                done.await()
                runCatching { client.close() }
                runCatching { upstream.close() }
                a.join(); b.join()
            } finally {
                runCatching { upstream.close() }
            }
        } finally {
            connections -= client
            runCatching { client.close() }
        }
    }

    private fun connectUpstream(): Socket? {
        val net = usbNetwork
        val localIp = usbLocalIp
        if (net == null && localIp == null) {
            Log.w(TAG, "upstream rejected: usb network not present")
            return null
        }
        val s = Socket()
        try {
            when {
                net != null -> runCatching { net.bindSocket(s) }
                    .onFailure { Log.w(TAG, "network.bindSocket failed: ${it.message}") }
                localIp != null -> runCatching { s.bind(InetSocketAddress(localIp, 0)) }
                    .onFailure { Log.w(TAG, "bind localIp failed: ${it.message}") }
            }
            s.tcpNoDelay = true
            s.connect(InetSocketAddress(TARGET_HOST, TARGET_PORT), CONNECT_TIMEOUT_MS)
            return s
        } catch (e: Exception) {
            Log.w(TAG, "upstream connect to $TARGET_HOST:$TARGET_PORT failed: ${e.message}")
            runCatching { s.close() }
            return null
        }
    }

    private suspend fun pump(input: InputStream, output: OutputStream) {
        val buf = ByteArray(BUFFER)
        while (true) {
            val n = try {
                input.read(buf)
            } catch (e: Exception) {
                break
            }
            if (n <= 0) break
            try {
                output.write(buf, 0, n)
                output.flush()
            } catch (e: Exception) {
                break
            }
        }
    }

    // ---------------- usb 网卡检测 / 热插拔 ----------------

    private fun registerUsbMonitor() {
        val cm = connMgr ?: return
        val request = try {
            NetworkRequest.Builder().clearCapabilities().build() // 匹配所有网络
        } catch (e: Exception) {
            Log.w(TAG, "build NetworkRequest failed: ${e.message}")
            return
        }
        val cb = object : ConnectivityManager.NetworkCallback() {
            override fun onAvailable(network: Network) { rescanUsb() }
            override fun onLost(network: Network) { rescanUsb() }
        }
        networkCallback = cb
        try {
            cm.registerNetworkCallback(request, cb)
        } catch (e: Exception) {
            Log.w(TAG, "registerNetworkCallback failed: ${e.message}")
        }
        // 轮询兜底：即使回调未覆盖，也能在 2s 内感知热插拔
        scope.launch {
            while (running) {
                delay(RESCAN_INTERVAL_MS)
                rescanUsb()
            }
        }
    }

    private fun unregisterUsbMonitor() {
        val cm = connMgr
        val cb = networkCallback
        if (cm != null && cb != null) {
            runCatching { cm.unregisterNetworkCallback(cb) }
        }
        networkCallback = null
    }

    /**
     * 重新探测 usb 网卡。优先级：
     *  1) ConnectivityManager 中 interfaceName 以 "usb" 开头的 Network
     *  2) NetworkInterface 枚举中名称以 "usb" 开头且已 up 的接口
     * 本地源地址取该接口的首个 IPv4。仅在状态变化（或 forceNotify）时回调，避免刷屏。
     */
    @Synchronized
    fun rescanUsb(forceNotify: Boolean = false) {
        val cm = connMgr
        var foundNet: Network? = null
        var name: String? = null

        if (cm != null) {
            try {
                for (n in cm.allNetworks) {
                    val iface = runCatching { cm.getLinkProperties(n)?.interfaceName }.getOrNull()
                    if (iface != null && iface.startsWith("usb", ignoreCase = true)) {
                        foundNet = n; name = iface; break
                    }
                }
            } catch (e: SecurityException) {
                // 缺 ACCESS_NETWORK_STATE 时跳过 ConnectivityManager，回退到 NetworkInterface 枚举
                Log.w(TAG, "ConnectivityManager access denied (ACCESS_NETWORK_STATE?): ${e.message}")
            }
        }
        if (name == null) {
            try {
                val ifaces = NetworkInterface.getNetworkInterfaces()
                if (ifaces != null) {
                    for (ni in ifaces) {
                        if (ni.isUp && !ni.isLoopback &&
                            ni.name.startsWith("usb", ignoreCase = true)
                        ) {
                            name = ni.name; break
                        }
                    }
                }
            } catch (_: Exception) {
            }
        }

        var localIp: InetAddress? = null
        if (name != null) {
            try {
                val ni = NetworkInterface.getByName(name)
                if (ni != null) {
                    val addrs = ni.inetAddresses
                    while (addrs.hasMoreElements()) {
                        val a = addrs.nextElement()
                        if (a is Inet4Address && !a.isLoopbackAddress) {
                            localIp = a; break
                        }
                    }
                }
            } catch (_: Exception) {
            }
        }

        val wasUp = usbNetwork != null || usbLocalIp != null
        usbNetwork = foundNet
        usbInterfaceName = name
        usbLocalIp = localIp
        val nowUp = foundNet != null || localIp != null

        if (forceNotify || nowUp != wasUp) {
            val (status, msg) = if (nowUp) {
                Status.USB_UP to "USB 已连接 (${name ?: "usb"})，正在转发 0.0.0.0:$listenPort → $TARGET_HOST:$TARGET_PORT"
            } else {
                Status.USB_DOWN to "USB 已断开，转发暂停（保持监听 0.0.0.0:$listenPort，等待 USB 插入）"
            }
            onStatus(status, msg)
        }
    }
}
