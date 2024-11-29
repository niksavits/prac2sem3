#include <iostream>
#include <thread>
#include <mutex>
#include <netinet/in.h>
#include <unistd.h>
#include <cstring>
#include <arpa/inet.h>
#include "database_code.h"

using namespace std;

#define PORT 7432

mutex db_mutex;

void handle_client(int client_socket, Database& db, const string& client_ip) {
    char buffer[1024] = {0};
    string client_query;

    while (true) {
        int bytes_read = read(client_socket, buffer, 1024);
        if (bytes_read <= 0) {
            cerr << "Ошибка при чтении запроса от клиента или клиент закрыл соединение." << endl;
            break;
        }
        client_query = string(buffer, bytes_read);

        if (client_query == "exit") {
            cout << "Клиент закрыл соединение." << endl;
            break;
        }
        cout << "С IP: " << client_ip << "был отправлен запрос: " << client_query << endl;

        lock_guard<mutex> lock(db_mutex);

        stringstream output_stream;
        streambuf* cout_buf = cout.rdbuf(); 
        cout.rdbuf(output_stream.rdbuf());
        SQLParser::execQuery(client_query, db);
        cout.rdbuf(cout_buf);

        string response = output_stream.str();
        cout << "Ответ с IP: " << client_ip << ": " << response << endl; 

        send(client_socket, response.c_str(), response.size(), 0);
    }

    close(client_socket);
}

int main() {
    int server_fd, client_socket;
    struct sockaddr_in address;
    int opt = 1;
    int addrlen = sizeof(address);

    Database db("/home/skywalker/Рабочий стол/output/scheme.json");

    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("Ошибка при создании сокета");
        exit(EXIT_FAILURE);
    }

    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
        perror("Ошибка установки параметров сокета");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("Ошибка привязки сокета");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    if (listen(server_fd, 10) < 0) {
        perror("Ошибка при прослушивании");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    cout << "Сервер запущен и ожидает соединений на порту " << PORT << endl;

    while (true) {
        if ((client_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen)) < 0) {
            perror("Ошибка принятия соединения");
            close(server_fd);
            exit(EXIT_FAILURE);
        }
        char client_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &address.sin_addr, client_ip, sizeof(client_ip));
        cout << "Подключение с IP: " << client_ip << endl;

        thread client_thread(handle_client, client_socket, ref(db), string(client_ip));
        client_thread.detach(); 
    }

    close(server_fd);

    return 0;
}