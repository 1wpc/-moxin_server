import java.io.BufferedReader
import java.io.IOException
import java.io.InputStreamReader
import java.io.PrintWriter
import java.net.ServerSocket
import java.net.Socket
import java.util.*
import com.google.gson.*
import com.google.gson.reflect.TypeToken

//fun main() {
//    println("ChatServer starting...")
//    val serverPort = 8848
//    val serverSocket = ServerSocket(serverPort)
//    println("Server has started on port $serverPort")
//    val socket = serverSocket.accept()
//    println("Client connected to port $serverPort")
//    val outputStream = socket.getOutputStream()
//    val inputStream = socket.getInputStream()
//    while (true){
//        outputStream.write(readLine()!!.toByteArray())
//    }
//}

fun jsonToMap(jsonString: String): Map<String, Any> {
    val gson = Gson()
    val typeToken = object : TypeToken<Map<String, Any>>() {}.type
    return gson.fromJson(jsonString, typeToken)
}

data class Message(val toId: String, val content: String)

class ClientHandler (val socket: Socket, val clientId: String, val messageQueue: Queue<Message>) : Runnable {
    var noneReadMsgs: Queue<Message> = LinkedList()
    init{
        noneReadMsgs.addAll(messageQueue)
    }
    override fun run() {
        try {
            val `in` = BufferedReader(InputStreamReader(socket.getInputStream()))
            val out = PrintWriter(socket.getOutputStream(), true)

            // 发送暂存的消息
            if (noneReadMsgs.isNotEmpty()) {
                noneReadMsgs.forEach { out.println(it.content) }
            }

            while (true) {
                println("listening $clientId")
                try {
                    val inputLine = `in`.readLine()
                    println("receive $clientId: $inputLine")
                    if (inputLine == null || inputLine == "bye"){
                        clients.remove(clientId)
                        break
                    }  // 检测断开连接

                    // 解析JSON
                    val data = jsonToMap(inputLine)

                    // 找到目标客户端，并发送消息
                    // 此处应有查找目标客户端的逻辑
                    if (clients.containsKey(data["roomId"])){
                        val out0 = PrintWriter(clients[data["roomId"]]?.socket?.getOutputStream(),true)
                        out0.println(inputLine)
                    }else{
                        println("对方下线")
                        boxQueue.add(Message(data["roomId"].toString(), inputLine))
                    }
                }catch (ex: IOException) {
                    ex.printStackTrace()
                    clients.remove(clientId)
                    socket.close()
                    break
                }
            }
        } catch (e: IOException) {
            e.printStackTrace()
        } finally {
            try {
                clients.remove(clientId);
                socket.close()
            } catch (e: IOException) {
                e.printStackTrace()
            }
        }
    }
}

val clients = mutableMapOf<String, ClientHandler>() // 存储所有客户端
val boxQueue = LinkedList<Message>()//信箱
val messageQueue = LinkedList<Message>()//某客户端的未收信件

fun main() {
    val serverSocket = ServerSocket(8848)

    while (true) {
        println("等待客户端连接...")
        val socket = serverSocket.accept()
        println("新的客户端连接！")


        // 这里应有认证过程，生成或确认clientId
        val clientId = BufferedReader(InputStreamReader(socket.getInputStream())).readLine()
        println(clientId)
        //找信
        boxQueue.forEach { ele ->
            if (ele.toId == clientId) {
                messageQueue.add(ele)
            }
        }

        // 创建并启动客户端处理线程
        val clientHandler = ClientHandler(socket, clientId, messageQueue)
        Thread(clientHandler).start()

        //清理信箱
        boxQueue.removeAll { ele -> ele.toId == clientId }
        messageQueue.clear()
        // 将客户端加入列表
        clients[clientId] = clientHandler
        println("$clientId done")
    }
}