#include <iostream>
#include <string>
#include <cstring>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>

using namespace std;

#define SERVER_IP "192.168.0.13"
#define SERVER_PORT 7432 
#define BUFFER_SIZE 1024

int main() {
    int sock = 0;
    struct sockaddr_in serv_addr;
    char buffer[BUFFER_SIZE] = {0};

    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) { // Стандарт айпишника по 4 символам
        cerr << "Ошибка создания сокета" << endl;
        return -1;
    }

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(SERVER_PORT); // порт в сетевой порядок байтов, чтобы данные передались корректно

    // айпишник из текста в двоичную форму
    if (inet_pton(AF_INET, SERVER_IP, &serv_addr.sin_addr) <= 0) {
        cerr << "Неправильный адрес/Адрес не поддерживается" << endl;
        return -1;
    }

    if (connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        cerr << "Ошибка подключения к серверу" << endl;
        return -1;
    }

    cout << "Подключено к серверу " << SERVER_IP << " на порту " << SERVER_PORT << endl;

    while (true) {
        string query;
        cout << "Введите SQL-запрос (или 'exit' для выхода): ";
        getline(cin, query);

        if (query == "exit") {
            break;
        }

        send(sock, query.c_str(), query.length(), 0);

        memset(buffer, 0, BUFFER_SIZE);

        int valread = read(sock, buffer, BUFFER_SIZE - 1);
        if (valread > 0) {
            buffer[valread] = '\0';
            cout << "Ответ сервера: " << buffer << endl;
        } else if (valread == 0) {
            cerr << "Сервер закрыл соединение." << endl;
        } else {
            cerr << "Ошибка при чтении ответа от сервера. Код ошибки: " << valread << endl;
        }
    }

    shutdown(sock, SHUT_RDWR);
    close(sock);
    cout << "Отключение от сервера" << endl;

    return 0;
}