#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#if defined(_MSVC_LANG)
#define GATIDB_CPLUSPLUS_VERSION _MSVC_LANG
#else
#define GATIDB_CPLUSPLUS_VERSION __cplusplus
#endif

#if GATIDB_CPLUSPLUS_VERSION < 202002L
#error "gatidb requires C++20. Build with CMake or pass -std=c++20 to the compiler."
#endif

#undef GATIDB_CPLUSPLUS_VERSION

namespace gatidb {

inline constexpr std::size_t PAGE_SIZE = 4096;
inline constexpr std::size_t VALUE_SIZE = 256;
inline constexpr std::size_t PAGE_HEADER_SIZE = 8;
inline constexpr std::size_t WAL_RECORD_SIZE = 8 + 4 + PAGE_SIZE;

using Page = std::array<std::uint8_t, PAGE_SIZE>;

struct Node {
    bool is_leaf = true;
    std::vector<std::int32_t> keys;
    std::vector<std::vector<std::uint8_t>> values;
    std::vector<std::uint32_t> children;
};

Page serialize_node(
    bool is_leaf,
    const std::vector<std::int32_t>& keys,
    const std::vector<std::vector<std::uint8_t>>& values,
    const std::vector<std::uint32_t>& children
);
Node deserialize_node(const Page& page);
std::uint64_t get_page_lsn(const Page& page);
void set_page_lsn(Page& page, std::uint64_t lsn);

class DiskManager {
public:
    explicit DiskManager(std::string filename);
    DiskManager(DiskManager&& other) noexcept;
    DiskManager& operator=(DiskManager&& other) noexcept;

    DiskManager(const DiskManager&) = delete;
    DiskManager& operator=(const DiskManager&) = delete;

    Page read_page(std::uint32_t page_id);
    void write_page(std::uint32_t page_id, const Page& data);
    void flush();

private:
    std::string filename_;
    std::fstream file_;
};

struct WalRecord {
    std::uint64_t lsn = 0;
    std::uint32_t page_id = 0;
    Page page_data{};

    std::array<std::uint8_t, WAL_RECORD_SIZE> serialize() const;
    static WalRecord deserialize(const std::array<std::uint8_t, WAL_RECORD_SIZE>& bytes);
};

class BufferPool;

class Wal {
public:
    explicit Wal(std::string filename);
    Wal(Wal&& other) noexcept;
    Wal& operator=(Wal&& other) noexcept;

    Wal(const Wal&) = delete;
    Wal& operator=(const Wal&) = delete;

    std::uint64_t log_page(std::uint32_t page_id, const Page& page_data);
    void flush();
    void flush_to(std::uint64_t target_lsn);
    std::uint64_t flushed_lsn() const;
    std::uint64_t current_lsn() const;
    std::vector<WalRecord> read_from(std::uint64_t start_lsn);

    static void recover(Wal& wal, DiskManager& disk, std::uint64_t checkpoint_lsn);
    static std::uint64_t checkpoint(BufferPool& pool);
    static void write_checkpoint_lsn(const std::string& filename, std::uint64_t lsn);
    static std::uint64_t read_checkpoint_lsn(const std::string& filename);

private:
    std::string filename_;
    std::fstream file_;
    std::uint64_t current_lsn_ = 0;
    std::uint64_t flushed_lsn_ = 0;
};

class BufferPool {
public:
    BufferPool(DiskManager disk, Wal wal, std::size_t capacity);

    const Page& get_page(std::uint32_t page_id);
    void write_page(std::uint32_t page_id, Page data);
    void flush();
    std::uint64_t current_lsn() const;

    std::size_t cached_page_count() const;
    bool contains_page(std::uint32_t page_id) const;
    bool dirty_empty() const;

private:
    void touch(std::uint32_t page_id);
    void evict();

    DiskManager disk_;
    Wal wal_;
    std::unordered_map<std::uint32_t, Page> pages_;
    std::unordered_map<std::uint32_t, bool> dirty_;
    std::unordered_map<std::uint32_t, std::uint64_t> page_lsn_;
    std::size_t capacity_ = 0;
    std::vector<std::uint32_t> lru_order_;
};

enum class DataTypeKind {
    Int,
    Varchar,
    Bool,
};

struct DataType {
    DataTypeKind kind = DataTypeKind::Int;
    std::size_t max_length = 0;

    static DataType Int();
    static DataType Varchar(std::size_t max_length);
    static DataType Bool();

    bool operator==(const DataType& other) const;
};

struct Column {
    std::string name;
    DataType data_type;
};

struct Value {
    std::variant<std::monostate, std::int32_t, std::string, bool> data;

    static Value Int(std::int32_t value);
    static Value Varchar(std::string value);
    static Value Bool(bool value);
    static Value Null();

    bool is_null() const;
    bool is_int() const;
    std::int32_t as_int() const;
    std::string to_string() const;

    bool operator==(const Value& other) const;
};

struct Schema {
    std::vector<Column> columns;
    std::size_t primary_key = 0;

    std::vector<std::uint8_t> encode_row(const std::vector<Value>& row) const;
    std::vector<Value> decode_row(const std::vector<std::uint8_t>& data) const;
};

class DiskBtree {
public:
    DiskBtree(std::shared_ptr<BufferPool> pool, std::size_t degree);
    DiskBtree(
        std::shared_ptr<BufferPool> pool,
        std::uint32_t root_page_id,
        std::uint32_t next_page_id,
        std::size_t degree
    );

    std::uint32_t next_page_id() const;
    std::uint32_t root_page_id() const;
    std::optional<std::vector<std::uint8_t>> search(std::int32_t key);
    void insert(std::int32_t key, std::vector<std::uint8_t> value);
    void erase(std::int32_t key);
    std::vector<std::pair<std::int32_t, std::vector<std::uint8_t>>> scan(std::int32_t start, std::int32_t end);
    std::vector<std::pair<std::int32_t, std::vector<std::uint8_t>>> scan_all();
    void flush();

private:
    std::optional<std::vector<std::uint8_t>> search_node(std::uint32_t page_id, std::int32_t key);
    void insert_non_full(std::uint32_t page_id, std::int32_t key, std::vector<std::uint8_t> value);
    void split_child(std::uint32_t parent_id, std::size_t idx);
    void delete_key(std::uint32_t page_id, std::int32_t key);
    std::pair<std::int32_t, std::vector<std::uint8_t>> get_predecessor(std::uint32_t page_id);
    std::pair<std::int32_t, std::vector<std::uint8_t>> get_successor(std::uint32_t page_id);
    void merge(std::uint32_t parent_id, std::size_t idx);
    void fill(std::uint32_t parent_id, std::size_t idx);
    void borrow_from_prev(std::uint32_t parent_id, std::size_t idx);
    void borrow_from_next(std::uint32_t parent_id, std::size_t idx);
    void scan_node(std::uint32_t page_id, std::int32_t start, std::int32_t end, std::vector<std::pair<std::int32_t, std::vector<std::uint8_t>>>& out);
    std::uint32_t allocate_page();

    std::shared_ptr<BufferPool> pool_;
    std::uint32_t root_page_id_ = 0;
    std::uint32_t next_page_id_ = 0;
    std::size_t degree_ = 0;
};

class Table {
public:
    Table(std::string name, Schema schema, std::shared_ptr<BufferPool> pool, std::size_t degree);
    Table(
        std::string name,
        Schema schema,
        std::shared_ptr<BufferPool> pool,
        std::uint32_t root_page_id,
        std::uint32_t next_page_id,
        std::size_t degree
    );

    const std::string& name() const;
    const Schema& schema() const;
    void insert_row(const std::vector<Value>& row);
    std::optional<std::vector<Value>> get_row(std::int32_t pk);
    void delete_row(std::int32_t pk);
    std::vector<std::vector<Value>> scan(std::int32_t start, std::int32_t end);
    std::uint32_t next_page_id() const;
    std::uint32_t root_page_id() const;
    void flush();

private:
    std::string name_;
    Schema schema_;
    DiskBtree tree_;
};

struct TableMeta {
    std::string name;
    Schema schema;
    std::uint32_t root_page_id = 0;
    std::size_t degree = 0;
};

class Catalog {
public:
    explicit Catalog(BufferPool pool);

    void create_table(const std::string& name, Schema schema, std::size_t degree);
    const TableMeta* get_table_meta(const std::string& name) const;
    std::optional<Table> get_table(const std::string& name) const;
    void update_next_page_id(std::uint32_t id);
    void update_table_storage(const Table& table);
    void flush();

    static Page serialize_catalog(const std::vector<TableMeta>& tables, std::uint32_t next_page_id);
    static std::pair<std::vector<TableMeta>, std::uint32_t> deserialize_catalog(const Page& page);

private:
    std::shared_ptr<BufferPool> pool_;
    std::vector<TableMeta> tables_;
    std::uint32_t next_page_id_ = 1;
};

namespace sql {

enum class TokenKind {
    Create,
    Table,
    Insert,
    Into,
    Values,
    Select,
    From,
    Where,
    Between,
    And,
    Int,
    Varchar,
    Bool,
    Primary,
    Key,
    True,
    False,
    Ident,
    IntLit,
    StrLit,
    LParen,
    RParen,
    Comma,
    Semicolon,
    Star,
    Eq,
    Eof,
};

struct Token {
    TokenKind kind = TokenKind::Eof;
    std::string text;
    std::int32_t int_value = 0;

    bool operator==(TokenKind expected) const;
};

struct ColumnDef {
    std::string name;
    DataType data_type;
};

struct CreateTable {
    std::string name;
    std::vector<ColumnDef> columns;
    std::size_t primary_key = 0;
};

struct Insert {
    std::string table;
    std::vector<Value> values;
};

enum class FilterKind {
    None,
    Eq,
    Between,
};

struct Filter {
    FilterKind kind = FilterKind::None;
    std::string column;
    Value left = Value::Null();
    Value right = Value::Null();
};

struct Select {
    std::string table;
    Filter filter;
};

using Statement = std::variant<CreateTable, Insert, Select>;

std::vector<Token> tokenize(const std::string& source);
Statement parse(std::vector<Token> tokens);

} // namespace sql

} // namespace gatidb
