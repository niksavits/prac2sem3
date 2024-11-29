#include "database_code.h"
#include <iostream>
#include <sstream>
#include <fstream>
#include <algorithm>
#include <string>
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

using namespace std;

// Реализация parsJson
parsJson::parsJson(const string& config_file) {
    ifstream file(config_file);
    if (!file) {
        cerr << "Не удалось открыть файл конфигурации: " << config_file << endl;
        return;
    }

    string line;
    while (getline(file, line)) {
        if (line.find("\"name\"") != string::npos) {
            name = cleanString(extractValue(line));
        } else if (line.find("\"tuples_limit\"") != string::npos) {
            tuples_limit = stoi(extractValue(line));
        } else if (line.find("\"table_name\"") != string::npos) {
            structure[structure_size].table_name = cleanString(extractValue(line));
        } else if (line.find("\"columns\"") != string::npos) {
            structure[structure_size].columns_count = extractColumns(file, structure[structure_size].columns);
            structure_size++;
        }
    }
}

string parsJson::extractValue(const string& line) {
    size_t pos = line.find(":");
    if (pos != string::npos) {
        string value = line.substr(pos + 1);
        value.erase(0, value.find_first_not_of(" \"\n\r"));
        value.erase(value.find_last_not_of(" \"\n\r,") + 1);
        return value;
    }
    return "";
}

string parsJson::cleanString(const string& line) {
    string value = line;
    if (!value.empty() && value[0] == '"') {
        value.erase(0, 1);
    }
    if (!value.empty() && (value.back() == '"' || value.back() == ',')) {
        value.pop_back();
    }
    value.erase(0, value.find_first_not_of(" "));
    value.erase(value.find_last_not_of(" ") + 1);
    return value;
}

int parsJson::extractColumns(ifstream& file, string columns[]) {
    int count = 0;
    string line;

    while (getline(file, line)) {
        line.erase(remove(line.begin(), line.end(), '\"'), line.end());
        line.erase(remove(line.begin(), line.end(), ','), line.end());
        line.erase(remove(line.begin(), line.end(), ' '), line.end());

        if (line == "]") {
            break;
        }

        if (!line.empty()) {
            columns[count++] = line;
        }
    }
    return count;
}

// Реализация TableLock
TableLock::TableLock() {
    pthread_mutex_init(&lock, nullptr);
}

TableLock::~TableLock() {
    pthread_mutex_destroy(&lock);
}

void TableLock::tableLock(const string& table_path) {
    pthread_mutex_lock(&lock);
    ofstream lock_file(table_path + "_lock");
    lock_file << "locked";
    lock_file.close();
}

void TableLock::tableUnlock(const string& table_path) {
    ofstream lock_file(table_path + "_lock");
    lock_file << "unlocked";
    lock_file.close();
    pthread_mutex_unlock(&lock);
}

// Реализация Table
Table::Table() : tuples_limit(0), pk_sequence(1) {}

Table::Table(const string& name, string cols[], int columns_count, int limit, const string& schema_name)
    : columns_count(columns_count), tuples_limit(limit), pk_sequence(1) {
    table_name = name;
    table_path = schema_name + "/" + table_name;
    mkdir(table_path.c_str(), 0777);

    for (int i = 0; i < columns_count; ++i) {
        columns[i] = cols[i];
    }

    ofstream pk_file(table_path + "/" + table_name + "_pk_sequence");
    pk_file << pk_sequence;
    pk_file.close();
}

string Table::getNextFile() {
    int file_number = 1;
    string filename;
    while (true) {
        filename = table_path + "/" + to_string(file_number) + ".csv";
        ifstream file(filename);
        if (!file) {
            break;
        }
        if (gerRowCount(filename) < tuples_limit) {
            return filename;
        }
        file_number++;
    }
    return table_path + "/" + to_string(file_number) + ".csv";
}

int Table::gerRowCount(const string& file) {
    ifstream infile(file);
    string line;
    int count = 0;
    while (getline(infile, line)) {
        count++;
    }
    return count - 1;
}

void Table::increment_pk() {
    pk_sequence++;
    ofstream pk_file(table_path + "/" + table_name + "_pk_sequence");
    pk_file << pk_sequence;
    pk_file.close();
}

void Table::insRow(string values[], int values_count) {
    table_lock.tableLock(table_path);

    string current_file = getNextFile();

    ifstream check_file(current_file);
    bool is_empty = check_file.peek() == ifstream::traits_type::eof();
    check_file.close();

    if (is_empty) {
        ofstream csv_out(current_file);
        csv_out << table_name + "_pk,";
        for (int i = 0; i < columns_count; ++i) {
            csv_out << columns[i];
            if (i != columns_count - 1) {
                csv_out << ",";
            }
        }
        csv_out << "\n";
        csv_out.close();
    }

    ofstream file_out(current_file, ios::app);
    file_out << pk_sequence << ",";
    for (int i = 0; i < values_count; ++i) {
        file_out << values[i];
        if (i != values_count - 1) {
            file_out << ",";
        }
    }
    file_out << "\n";
    file_out.close();

    increment_pk();
    table_lock.tableUnlock(table_path);
}

void Table::delRow(const string& condition) {
    table_lock.tableLock(table_path);

    string temp_file_path = table_path + "/temp.csv";
    string file_path = getNextFile();
    ifstream infile(file_path);
    ofstream temp_file(temp_file_path);
    string line;

    getline(infile, line);
    temp_file << line << "\n";

    while (getline(infile, line)) {
        if (!testWhere(line, condition)) {
            temp_file << line << "\n";
        }
    }

    infile.close();
    temp_file.close();

    remove(file_path.c_str());
    rename(temp_file_path.c_str(), file_path.c_str());

    table_lock.tableUnlock(table_path);
}

void Table::selectRows(const string columns[], int col_count, const string& where_clause) {
    string file_path = getNextFile();
    ifstream infile(file_path);

    if (!infile) {
        cerr << "Ошибка: Не удалось открыть файл " << file_path << endl;
        return;
    }

    string line;
    bool is_first_line = true;
    bool has_output = false;

    while (getline(infile, line)) {
        if (is_first_line) {
            is_first_line = false;
            continue;
        }
        if (testWhere(line, where_clause)) {
            if (!has_output) {
                cout << "Вывод выбранных колонок:" << endl;
                has_output = true;
            }
            printSelCol(line, columns, col_count);
        }
    }

    if (!has_output) {
        cout << "Нет данных, соответствующих условиям." << endl;
    }

    infile.close();
}

int Table::getIndColumn(const string& column_name) {
    string actual_column;
    size_t dot_pos = column_name.find('.');
    if (dot_pos != string::npos) {
        actual_column = column_name.substr(dot_pos + 1);
    } else {
        actual_column = column_name;
    }

    for (int i = 0; i < columns_count; ++i) {
        if (columns[i] == actual_column) {
            return i + 1;
        }
    }
    return -1;
}

bool isNumber(const string& str) {
    if (str.empty()) return false;
    for (char c : str) {
        if (!isdigit(c)) return false;
    }
    return true;
}

bool Table::testWhere(const string& row, const string& where_clause) {
    if (where_clause.empty()) return true;

    size_t pos_eq = where_clause.find('=');
    size_t pos_gt = where_clause.find('>');
    size_t pos_lt = where_clause.find('<');
    size_t pos = pos_eq != string::npos ? pos_eq : (pos_gt != string::npos ? pos_gt : pos_lt);

    string column_name = where_clause.substr(0, pos);
    string value = where_clause.substr(pos + 1);
    column_name.erase(remove(column_name.begin(), column_name.end(), ' '), column_name.end());
    value.erase(remove(value.begin(), value.end(), ' '), value.end());
    value.erase(remove(value.begin(), value.end(), '\''), value.end());

    int col_index = getIndColumn(column_name);

    stringstream row_ss(row);
    string cell;
    int current_index = 0;

    while (getline(row_ss, cell, ',')) {
        cell.erase(remove(cell.begin(), cell.end(), ' '), cell.end());
        cell.erase(remove(cell.begin(), cell.end(), '\"'), cell.end());

        if (current_index == col_index) {
            if (pos_eq != string::npos) {
                return cell == value;
            } else if (pos_gt != string::npos) {
                if (isNumber(cell) && isNumber(value)) {
                    return stoi(cell) > stoi(value);
                }
                return false;
            } else if (pos_lt != string::npos) {
                if (isNumber(cell) && isNumber(value)) {
                    return stoi(cell) < stoi(value);
                }
                return false;
            }
        }
        current_index++;
    }

    return false;
}

void Table::printSelCol(const string& row, const string columns[], int col_count) {
    stringstream ss(row);
    string cell;
    int col_idx = 0;
    bool is_first_row = true;

    while (getline(ss, cell, ',')) {
        if (is_first_row) {
            is_first_row = false;
            col_idx++;
            continue;
        }

        if (col_idx - 1 < col_count) {
            cout << columns[col_idx - 1] << " - " << cell << endl;
        }
        col_idx++;
    }
}

Database::Database(const string& config_file) {
    parsJson schema(config_file);

    schema_name = schema.name;
    tuples_limit = schema.tuples_limit;

    mkdir(schema_name.c_str(), 0777);

    for (int i = 0; i < schema.structure_size; ++i) {
        tables[tables_count++] = new Table(schema.structure[i].table_name, schema.structure[i].columns, 
                                   schema.structure[i].columns_count, tuples_limit, schema_name);
    }
}

Table* Database::find_table(const string& table_name) {
    for (int i = 0; i < tables_count; ++i) {
        if (tables[i]->table_name == table_name) {
            return tables[i];
        }
    }
    return nullptr;
}

void Database::insINTO(const string& table_name, string values[], int values_count) {
    Table* table = find_table(table_name);
    if (table) {
        table->insRow(values, values_count);
    } else {
        cerr << "Таблица не найдена: " << table_name << endl;
    }
}

void Database::delFROM(const string& table_name, const string& condition) {
    Table* table = find_table(table_name);
    if (table) {
        table->delRow(condition);
    } else {
        cerr << "Таблица не найдена!" << endl;
    }
}

void Database::selectFROM(const string& table_name, const string columns[], int col_count, const string& where_clause) {
    Table* table = find_table(table_name);
    if (table) {
        table->selectRows(columns, col_count, where_clause);
    } else {
        cerr << "Таблица не найдена!" << endl;
    }
}

void Database::selectFROMmult(const string& table_name1, const string& table_name2, const string columns[], int col_count, const string& where_clause) {
    Table* table1 = find_table(table_name1);
    Table* table2 = find_table(table_name2);

    if (!table1 || !table2) {
        cerr << "Одна или обе таблицы не найдены." << endl;
        return;
    }

    string file_path1 = table1->getNextFile();
    string file_path2 = table2->getNextFile();

    ifstream infile1(file_path1);
    ifstream infile2(file_path2);

    string rows1[MAX_ROWS];
    string rows2[MAX_ROWS];
    int count1 = 0;
    int count2 = 0;

    while (getline(infile1, rows1[count1]) && count1 < MAX_ROWS) count1++;
    while (getline(infile2, rows2[count2]) && count2 < MAX_ROWS) count2++;

    bool has_output = false;

    for (int i = 1; i < count1; i++) {
        for (int j = 1; j < count2; j++) {
            string combined_row = rows1[i] + "," + rows2[j];
            if (table1->testWhere(combined_row, where_clause)) {
                if (!has_output) {
                    cout << "Вывод выбранных колонок из объединенных таблиц:" << endl;
                    has_output = true;
                }
                table1->printSelCol(combined_row, columns, col_count);
            }
        }
    }

    if (!has_output) {
        cout << "Нет данных, соответствующих условиям." << endl;
    }

    infile1.close();
    infile2.close();
}

// Реализация SQLParser
void SQLParser::execQuery(const string& query, Database& db) {
    istringstream iss(query);
    string command;
    iss >> command;

    if (command == "INSERT") {
        handleIns(iss, db);
    } else if (command == "SELECT") {
        handleSelect(iss, db);
    } else if (command == "DELETE") {
        handleDel(iss, db);
    } else {
        cerr << "Неизвестная SQL-команда: " << command << endl;
    }
}

void SQLParser::handleIns(istringstream& iss, Database& db) {
    string into, table_name, values;
    iss >> into >> table_name;

    string row_data;
    getline(iss, row_data);

    size_t start = row_data.find("(");
    size_t end = row_data.find(")");
    if (start != string::npos && end != string::npos && end > start) {
        row_data = row_data.substr(start + 1, end - start - 1);
    } else {
        cerr << "Ошибка в синтаксисе INSERT-запроса." << endl;
        return;
    }

    string row_values[MAX_COLUMNS];
    int values_count = 0;

    stringstream ss(row_data);
    string value;
    while (getline(ss, value, ',')) {
        value.erase(remove(value.begin(), value.end(), '\"'), value.end());
        value.erase(remove(value.begin(), value.end(), ' '), value.end());
        row_values[values_count++] = value;
    }

    db.insINTO(table_name, row_values, values_count);
    cout << "Команда INSERT выполнена успешно" << endl;
}

void SQLParser::handleDel(istringstream& iss, Database& db) {
    string from, table_name, condition;
    iss >> from >> table_name;
    getline(iss, condition);

    if (!condition.empty() && condition.find("WHERE") != string::npos) {
        condition = condition.substr(condition.find("WHERE") + 6);
    } else {
        condition.clear();
    }

    db.delFROM(table_name, condition);
    cout << "Команда DELETE выполнена успешно" << endl;
}

void SQLParser::handleSelect(istringstream& iss, Database& db) {
    string select_part, from_part, where_clause;
    string query = iss.str();

    size_t from_pos = query.find("FROM");
    size_t where_pos = query.find("WHERE");

    if (from_pos == string::npos) {
        cerr << "Ошибка: Ключевое слово FROM не найдено в запросе." << endl;
        return;
    }

    select_part = query.substr(0, from_pos);
    if (where_pos != string::npos) {
        from_part = query.substr(from_pos + 5, where_pos - from_pos - 5);
        where_clause = query.substr(where_pos + 6);
    } else {
        from_part = query.substr(from_pos + 5);
        where_clause = "";
    }

    space(select_part);
    space(from_part);
    space(where_clause);

    string columns_str;
    istringstream select_iss(select_part);
    select_iss >> columns_str;
    if (columns_str != "SELECT") {
        cerr << "Ошибка: Ожидалось ключевое слово SELECT." << endl;
        return;
    }

    string column_names = select_part.substr(select_part.find("SELECT") + 7);

    LinkedList parsed_columns;

    if (column_names == "*") {
        space(from_part);
        string table_name = from_part;
        Table* table = db.find_table(table_name);
        if (!table) {
            cerr << "Таблица не найдена: " << table_name << endl;
            return;
        }

        for (int i = 0; i < table->columns_count; ++i) {
            parsed_columns.push_back(table->columns[i]);
        }
    } else {
        parseCol(column_names, parsed_columns);
    }

    istringstream from_iss(from_part);
    string table_name;
    LinkedList table_names;

    while (getline(from_iss, table_name, ',')) {
        space(table_name);
        table_names.push_back(table_name);
    }

    LinkedList tables;
    for (int i = 0; i < table_names.size; ++i) {
        Table* table = db.find_table(table_names.get(i));
        if (table) {
            tables.push_back(table->table_name);
        } else {
            cerr << "Таблица не найдена: " << table_names.get(i) << endl;
            return;
        }
    }

    if (table_names.size == 1) {
        string* columns_array = new string[parsed_columns.size];
        for (int i = 0; i < parsed_columns.size; ++i) {
            columns_array[i] = parsed_columns.get(i);
        }

        db.selectFROM(tables.get(0), columns_array, parsed_columns.size, where_clause);

        delete[] columns_array;
    } else if (table_names.size == 2) {
        string* columns_array = new string[parsed_columns.size];
        for (int i = 0; i < parsed_columns.size; ++i) {
            columns_array[i] = parsed_columns.get(i);
        }

        db.selectFROMmult(tables.get(0), tables.get(1), columns_array, parsed_columns.size, where_clause);

        delete[] columns_array;
    } else {
        cerr << "Ошибка: Запрос может поддерживать только одну или две таблицы." << endl;
    }
}

void SQLParser::parseCol(const string& columns_str, LinkedList& parsed_columns) {
    stringstream ss(columns_str);
    string column;
    while (getline(ss, column, ',')) {
        space(column);
        parsed_columns.push_back(column);
    }
}

void SQLParser::space(string& str) {
    size_t first = str.find_first_not_of(' ');
    size_t last = str.find_last_not_of(' ');
    if (first != string::npos && last != string::npos) {
        str = str.substr(first, (last - first + 1));
    } else {
        str.clear();
    }
}

// Реализация LinkedList и Node
Node::Node(const string& value) : data(value), next(nullptr) {}

LinkedList::LinkedList() : head(nullptr), size(0) {}

void LinkedList::push_back(const string& value) {
    Node* newNode = new Node(value);
    if (!head) {
        head = newNode;
    } else {
        Node* current = head;
        while (current->next) {
            current = current->next;
        }
        current->next = newNode;
    }
    size++;
}

string& LinkedList::get(int index) {
    if (index < 0 || index >= size) {
        throw out_of_range("Index out of range");
    }
    Node* current = head;
    for (int i = 0; i < index; ++i) {
        current = current->next;
    }
    return current->data;
}

void LinkedList::print() const {
    Node* current = head;
    while (current) {
        cout << current->data << " ";
        current = current->next;
    }
    cout << endl;
}

LinkedList::~LinkedList() {
    Node* current = head;
    while (current) {
        Node* nextNode = current->next;
        delete current;
        current = nextNode;
    }
}