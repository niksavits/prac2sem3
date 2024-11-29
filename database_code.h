#ifndef DATABASE_CODE_H
#define DATABASE_CODE_H

#include <iostream>
#include <algorithm>
#include <string>
#include <sstream>
#include <fstream>
#include <pthread.h>
#include <sys/stat.h>
#include <dirent.h>
#include <cstring>
#include <unistd.h>

#define MAX_TABLES 100
#define MAX_COLUMNS 256
#define MAX_ROWS 1000

using namespace std;

// Структура parsJson для чтения конфигурации базы данных
struct parsJson {
    string name;
    int tuples_limit;
    struct {
        string table_name;
        string columns[MAX_COLUMNS];
        int columns_count;
    } structure[MAX_TABLES];
    int structure_size = 0;

    parsJson(const string& config_file);
    string extractValue(const string& line);
    string cleanString(const string& line);
    int extractColumns(ifstream& file, string columns[]);
};

// Класс TableLock для блокировки таблиц
struct TableLock {
    pthread_mutex_t lock;

    TableLock();
    ~TableLock();
    void tableLock(const string& table_path);
    void tableUnlock(const string& table_path);
};

// Класс Table для представления таблицы базы данных
struct Table {
    string table_name;
    string columns[MAX_COLUMNS];
    int columns_count;
    int tuples_limit;
    string table_path;
    TableLock table_lock;
    int pk_sequence;

    Table();
    Table(const string& name, string cols[], int columns_count, int limit, const string& schema_name);

    string getNextFile();
    int gerRowCount(const string& file);
    void increment_pk();
    void insRow(string values[], int values_count);
    void delRow(const string& condition);
    void selectRows(const string columns[], int col_count, const string& where_clause);
    int getIndColumn(const string& column_name);
    bool testWhere(const string& row, const string& where_clause);
    void printSelCol(const string& row, const string columns[], int col_count);
};

// Класс Database для представления базы данных
struct Database {
    string schema_name;
    int tuples_limit;
    Table* tables[MAX_TABLES];
    int tables_count = 0;

    Database(const std::string& config_file);  // Объявление конструктора

    Table* find_table(const std::string& table_name);
    void insINTO(const std::string& table_name, std::string values[], int values_count);
    void delFROM(const std::string& table_name, const std::string& condition);
    void selectFROM(const std::string& table_name, const std::string columns[], int col_count, const std::string& where_clause);
    void selectFROMmult(const std::string& table_name1, const std::string& table_name2, const std::string columns[], int col_count, const std::string& where_clause);
};

// Структура Node и класс LinkedList для работы с SQL-запросами
struct Node {
    string data;
    Node* next;

    Node(const string& value);
};

struct LinkedList {
    Node* head;
    int size;

    LinkedList();
    void push_back(const string& value);
    string& get(int index);
    void print() const;
    ~LinkedList();
};

// Класс SQLParser для обработки SQL-запросов
struct SQLParser {
    static void execQuery(const string& query, Database& db);

private:
    static void handleIns(istringstream& iss, Database& db);
    static void handleDel(istringstream& iss, Database& db);
    static void handleSelect(istringstream& iss, Database& db);
    static void parseCol(const string& columns_str, LinkedList& parsed_columns);
    static void space(string& str);
};

#endif