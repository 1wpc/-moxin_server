import java.io.BufferedReader
import java.io.IOException
import java.io.InputStreamReader
import java.io.PrintWriter
import java.net.ServerSocket
import java.net.Socket
import java.util.*
import com.google.gson.*
import com.google.gson.reflect.TypeToken
import java.sql.Connection
import java.sql.DriverManager

fun messageWrapper(content: String):String{
    return mapToJson(mapOf("info" to "normal", "data" to content))
}

fun createConnection(): Connection {
    //Class.forName("com.mysql.jdbc.Driver")
    val url = "jdbc:mysql://localhost:3306/?useSSL=false"
    val user = "root" // 默认管理员用户名
    val password = "libadmin" // 你的 root 密码
    return DriverManager.getConnection(url, user, password)
}

fun createDatabase(connection: Connection) {
    val sql = "CREATE DATABASE IF NOT EXISTS user_data"
    connection.prepareStatement(sql).use { it.executeUpdate() }
}

fun useDatabase(connection: Connection) {
    val sql = "USE user_data"
    connection.prepareStatement(sql).use { it.executeUpdate() }
}

fun createUserTable(connection: Connection) {
    val sql = """
        CREATE TABLE IF NOT EXISTS users (
        id VARCHAR(255) NOT NULL PRIMARY KEY,
    firstName VARCHAR(255),
    lastName VARCHAR(255),
    imageUrl VARCHAR(255),
    role ENUM('admin', 'user', 'guest') DEFAULT NULL,
    metadata JSON,
    createdAt INT,
    updatedAt INT,
    lastSeen INT
        )
    """.trimIndent()
    connection.prepareStatement(sql).use { it.executeUpdate() }
}

fun getUserById(connection: Connection, userId: String): Map<String, Any>? {
    val sql = "SELECT * FROM users WHERE id = ?"
    connection.prepareStatement(sql).use { statement ->
        statement.setString(1, userId)
        statement.executeQuery().use { resultSet ->
            if (resultSet.next()) {
                return mapOf(
                    "id" to resultSet.getString("id"),
                    "firstName" to resultSet.getString("firstName"),
                    "lastName" to resultSet.getString("lastName"),
                    "imageUrl" to resultSet.getString("imageUrl"),
                    "role" to resultSet.getString("role"),
                    "metadata" to resultSet.getString("metadata"), // Assuming metadata is stored as JSON string
                    "createdAt" to resultSet.getInt("createdAt"),
                    "updatedAt" to resultSet.getInt("updatedAt"),
                    "lastSeen" to resultSet.getInt("lastSeen")
                )
            }
        }
    }
    return null
}

fun addUser(connection: Connection, user: Map<String, Any>) {
    val sql = """
        INSERT INTO users (id, firstName, lastName, imageUrl, role, metadata, createdAt, updatedAt, lastSeen)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """.trimIndent()

    connection.prepareStatement(sql).use { statement ->
        statement.setString(1, user["id"].toString())
        statement.setString(2, user["firstName"].toString())
        statement.setString(3, null)
        statement.setString(4, null)
        statement.setString(5, null)
        statement.setString(6, mapToJson(user["metadata"] as Map<*, *>))
        statement.setInt(7, (user["createdAt"]  ?: 0) as Int)
        statement.setInt(8, (user["updatedAt"] ?: 0) as Int)
        statement.setInt(9, (user["lastSeen"]  ?: 0) as Int)

        statement.executeUpdate()
    }
}

fun mapToJson(map: Map<*, *>): String {
    val gson = Gson()
    return gson.toJson(map)
}


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
                noneReadMsgs.forEach { out.println(messageWrapper(it.content)) }
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
                    if (data["cmd"] == "get_public_key"){
                        val userId = data["data"] as String
                        val user_map = getUserById(connection, userId)
                        val metadata = jsonToMap(user_map?.get("metadata") as String)
                        val public_key = metadata["publicKey"] as String
                        out.println(mapToJson(mapOf("info" to "public_key", "return" to public_key, "extra" to userId)))
                    }else{
                        // 找到目标客户端，并发送消息
                        if (clients.containsKey(data["roomId"])){
                            val out0 = PrintWriter(clients[data["roomId"]]?.socket?.getOutputStream(),true)
                            out0.println(messageWrapper(inputLine))
                        }else{
                            println("对方下线")
                            boxQueue.add(Message(data["roomId"].toString(), inputLine))
                        }
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
lateinit var connection:Connection

fun main() {
    connection = createConnection()
    createDatabase(connection)
    useDatabase(connection)
    createUserTable(connection)

    val serverSocket = ServerSocket(8848)

    while (true) {
        println("等待客户端连接...")
        val socket = serverSocket.accept()
        println("新的客户端连接！")


        // 认证过程，确认clientId
        val data_pack = jsonToMap(BufferedReader(InputStreamReader(socket.getInputStream())).readLine())
        val user_map = data_pack["data"] as Map<String, Any>
        val cmd = data_pack["cmd"] as String
        val clientId = user_map["id"].toString()
        val printWriter = PrintWriter(socket.getOutputStream(), true)
        println(user_map)
        if (cmd == "verify_user"){
            val user_storage_map = getUserById(connection, clientId)
            if (user_storage_map != null){
                val metadataStorage = jsonToMap(user_storage_map["metadata"].toString()) as Map<*, *>
                val metadataUser = user_map["metadata"] as Map<*, *>
                if (metadataStorage["password"] != metadataUser["password"]){
                    printWriter.println(mapToJson(mapOf("info" to "error_pw")))
                    socket.close()
                    continue
                }
                printWriter.println(mapToJson(mapOf("info" to "success_verify")))
            }else{
                printWriter.println(mapToJson(mapOf("info" to "error_null_user")))
                socket.close()
                continue
            }
        }else if (cmd == "init_user"){
            val user_storage_map = getUserById(connection, clientId)
            if (user_storage_map == null){
                addUser(connection, user_map)
                printWriter.println(mapToJson(mapOf("info" to "success_init")))
            }else{
                printWriter.println(mapToJson(mapOf("info" to "error_user_repeat")))
                socket.close()
                continue
            }
        }

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