#include "gatidb/gatidb.h"

#include <algorithm>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#define CHECK(expr) \
    do { \
        if (!(expr)) { \
            throw std::runtime_error(std::string("check failed: ") + #expr); \
        } \
    } while (false)

namespace {

using namespace gatidb;

std::vector<std::uint8_t> bytes(const std::string& text) {
    return std::vector<std::uint8_t>(text.begin(), text.end());
}

void cleanup(const std::vector<std::string>& files) {
    for (const auto& file : files) {
        std::filesystem::remove(file);
    }
}

BufferPool make_pool(const std::string& db, const std::string& wal, std::size_t capacity) {
    cleanup({db, wal});
    return BufferPool(DiskManager(db), Wal(wal), capacity);
}

std::shared_ptr<BufferPool> make_shared_pool(const std::string& db, const std::string& wal, std::size_t capacity) {
    return std::make_shared<BufferPool>(make_pool(db, wal, capacity));
}

Schema users_schema() {
    return Schema{
        {
            Column{"id", DataType::Int()},
            Column{"name", DataType::Varchar(64)},
            Column{"age", DataType::Int()},
            Column{"active", DataType::Bool()},
        },
        0,
    };
}

void test_page_roundtrip() {
    Page page = serialize_node(true, {10, 20, 30}, {bytes("hello"), bytes("world"), bytes("foo")}, {});
    set_page_lsn(page, 42);
    CHECK(get_page_lsn(page) == 42);

    Node node = deserialize_node(page);
    CHECK(node.is_leaf);
    CHECK((node.keys == std::vector<std::int32_t>{10, 20, 30}));
    CHECK(node.values[1] == bytes("world"));
}

void test_disk_read_write() {
    const std::string db = "cpp_test_disk.db";
    cleanup({db});
    DiskManager disk(db);

    Page page{};
    page[8] = 42;
    page[4095] = 99;
    disk.write_page(3, page);

    Page out = disk.read_page(3);
    CHECK(out[8] == 42);
    CHECK(out[4095] == 99);
    CHECK(disk.read_page(99)[8] == 0);

    cleanup({db});
}

void test_wal_log_read_recover() {
    const std::string db = "cpp_test_wal.db";
    const std::string wal_file = "cpp_test_wal.wal";
    cleanup({db, wal_file});

    {
        Wal wal(wal_file);
        Page page{};
        page[100] = 17;
        CHECK(wal.log_page(0, page) == 0);
        page[100] = 29;
        CHECK(wal.log_page(1, page) == 1);
        CHECK(wal.read_from(0).size() == 2);
        wal.flush();
    }

    {
        Wal wal(wal_file);
        DiskManager disk(db);
        Wal::recover(wal, disk, 0);
    }

    DiskManager disk(db);
    CHECK(disk.read_page(0)[100] == 17);
    CHECK(disk.read_page(1)[100] == 29);
    cleanup({db, wal_file});
}

void test_buffer_pool_eviction() {
    const std::string db = "cpp_test_buffer.db";
    const std::string wal_file = "cpp_test_buffer.wal";
    BufferPool pool = make_pool(db, wal_file, 2);

    Page p0{};
    p0[8] = 10;
    Page p1{};
    p1[8] = 20;
    Page p2{};
    p2[8] = 30;

    pool.write_page(0, p0);
    pool.write_page(1, p1);
    pool.write_page(2, p2);

    CHECK(pool.cached_page_count() == 2);
    CHECK(!pool.contains_page(0));
    CHECK(pool.get_page(0)[8] == 10);
    CHECK(get_page_lsn(pool.get_page(0)) == 0);

    pool.flush();
    CHECK(pool.dirty_empty());
    cleanup({db, wal_file});
}

void test_btree_insert_search_scan_delete() {
    const std::string db = "cpp_test_btree.db";
    const std::string wal_file = "cpp_test_btree.wal";
    auto pool = make_shared_pool(db, wal_file, 64);
    DiskBtree tree(pool, 2);

    const std::vector<int> order = {13, 5, 17, 2, 9, 0, 14, 8, 3, 11, 19, 7, 15, 1, 12, 18, 6, 10, 4, 16};
    for (int key : order) {
        tree.insert(key, bytes("v" + std::to_string(key)));
    }

    for (int key = 0; key < 20; ++key) {
        CHECK(tree.search(key).value() == bytes("v" + std::to_string(key)));
    }

    auto all = tree.scan_all();
    CHECK(all.size() == 20);
    for (std::size_t i = 0; i < all.size(); ++i) {
        CHECK(all[i].first == static_cast<std::int32_t>(i));
    }

    auto subset = tree.scan(5, 10);
    CHECK(subset.size() == 5);
    CHECK(subset.front().first == 5);
    CHECK(subset.back().first == 9);

    for (int key = 0; key < 20; key += 2) {
        tree.erase(key);
    }
    for (int key = 0; key < 20; ++key) {
        const bool exists = tree.search(key).has_value();
        CHECK(exists == (key % 2 == 1));
    }

    cleanup({db, wal_file});
}

void test_schema_table_and_catalog_persistence() {
    const std::string db = "cpp_test_catalog.db";
    const std::string wal_file = "cpp_test_catalog.wal";
    cleanup({db, wal_file});

    {
        BufferPool pool(DiskManager(db), Wal(wal_file), 64);
        Catalog catalog(std::move(pool));
        catalog.create_table("users", users_schema(), 3);

        auto table = catalog.get_table("users").value();
        for (int id = 0; id < 50; ++id) {
            table.insert_row({
                Value::Int(id),
                Value::Varchar("user_" + std::to_string(id)),
                Value::Int(20 + id),
                Value::Bool(id % 2 == 0),
            });
        }

        catalog.update_table_storage(table);
        catalog.flush();
    }

    {
        BufferPool pool(DiskManager(db), Wal(wal_file), 64);
        Catalog catalog(std::move(pool));
        auto table = catalog.get_table("users").value();

        auto row = table.get_row(42).value();
        CHECK(row[0] == Value::Int(42));
        CHECK(row[1] == Value::Varchar("user_42"));
        CHECK(row[2] == Value::Int(62));
        CHECK(row[3] == Value::Bool(true));

        auto rows = table.scan(10, 15);
        CHECK(rows.size() == 5);
        CHECK(rows.front()[0] == Value::Int(10));
        CHECK(rows.back()[0] == Value::Int(14));
        CHECK(!table.get_row(99).has_value());
    }

    cleanup({db, wal_file});
}

void test_sql_tokenizer_and_parser() {
    using namespace gatidb::sql;

    auto create = parse(tokenize("CREATE TABLE jobs (id INT PRIMARY KEY, title VARCHAR(64), done BOOL);"));
    auto create_table = std::get<CreateTable>(create);
    CHECK(create_table.name == "jobs");
    CHECK(create_table.primary_key == 0);
    CHECK(create_table.columns.size() == 3);
    CHECK(create_table.columns[1].data_type == DataType::Varchar(64));

    auto insert = parse(tokenize("insert into jobs values (1, 'fix bug', false)"));
    auto insert_stmt = std::get<Insert>(insert);
    CHECK(insert_stmt.table == "jobs");
    CHECK(insert_stmt.values[0] == Value::Int(1));
    CHECK(insert_stmt.values[1] == Value::Varchar("fix bug"));
    CHECK(insert_stmt.values[2] == Value::Bool(false));

    auto select = parse(tokenize("SELECT * FROM jobs WHERE id BETWEEN 5 AND 20;"));
    auto select_stmt = std::get<Select>(select);
    CHECK(select_stmt.table == "jobs");
    CHECK(select_stmt.filter.kind == FilterKind::Between);
    CHECK(select_stmt.filter.left == Value::Int(5));
    CHECK(select_stmt.filter.right == Value::Int(20));
}

} // namespace

int main() {
    const std::vector<std::pair<std::string, void (*)()>> tests = {
        {"page_roundtrip", test_page_roundtrip},
        {"disk_read_write", test_disk_read_write},
        {"wal_log_read_recover", test_wal_log_read_recover},
        {"buffer_pool_eviction", test_buffer_pool_eviction},
        {"btree_insert_search_scan_delete", test_btree_insert_search_scan_delete},
        {"schema_table_and_catalog_persistence", test_schema_table_and_catalog_persistence},
        {"sql_tokenizer_and_parser", test_sql_tokenizer_and_parser},
    };

    for (const auto& [name, test] : tests) {
        try {
            test();
            std::cout << "[PASS] " << name << '\n';
        } catch (const std::exception& error) {
            std::cerr << "[FAIL] " << name << ": " << error.what() << '\n';
            return 1;
        }
    }

    return 0;
}
