#include "gatidb/gatidb.hpp"

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <utility>

namespace gatidb {
namespace {

[[noreturn]] void fail(const std::string& message) {
    throw std::runtime_error(message);
}

void require(bool condition, const std::string& message) {
    if (!condition) {
        fail(message);
    }
}

void ensure_capacity(std::size_t offset, std::size_t bytes) {
    if (offset + bytes > PAGE_SIZE) {
        fail("page serialization overflow");
    }
}

void write_u16(Page& page, std::size_t& offset, std::uint16_t value) {
    ensure_capacity(offset, 2);
    page[offset++] = static_cast<std::uint8_t>(value & 0xff);
    page[offset++] = static_cast<std::uint8_t>((value >> 8) & 0xff);
}

void write_u32(Page& page, std::size_t& offset, std::uint32_t value) {
    ensure_capacity(offset, 4);
    for (int i = 0; i < 4; ++i) {
        page[offset++] = static_cast<std::uint8_t>((value >> (i * 8)) & 0xff);
    }
}

void write_i32(Page& page, std::size_t& offset, std::int32_t value) {
    write_u32(page, offset, static_cast<std::uint32_t>(value));
}

void write_u64(Page& page, std::size_t offset, std::uint64_t value) {
    ensure_capacity(offset, 8);
    for (int i = 0; i < 8; ++i) {
        page[offset + static_cast<std::size_t>(i)] = static_cast<std::uint8_t>((value >> (i * 8)) & 0xff);
    }
}

std::uint16_t read_u16(const Page& page, std::size_t& offset) {
    ensure_capacity(offset, 2);
    std::uint16_t value =
        static_cast<std::uint16_t>(page[offset]) | (static_cast<std::uint16_t>(page[offset + 1]) << 8);
    offset += 2;
    return value;
}

std::uint32_t read_u32(const Page& page, std::size_t& offset) {
    ensure_capacity(offset, 4);
    std::uint32_t value = 0;
    for (int i = 0; i < 4; ++i) {
        value |= static_cast<std::uint32_t>(page[offset + static_cast<std::size_t>(i)]) << (i * 8);
    }
    offset += 4;
    return value;
}

std::int32_t read_i32(const Page& page, std::size_t& offset) {
    return static_cast<std::int32_t>(read_u32(page, offset));
}

void write_u64_to_bytes(std::array<std::uint8_t, WAL_RECORD_SIZE>& out, std::size_t offset, std::uint64_t value) {
    for (int i = 0; i < 8; ++i) {
        out[offset + static_cast<std::size_t>(i)] = static_cast<std::uint8_t>((value >> (i * 8)) & 0xff);
    }
}

void write_u32_to_bytes(std::array<std::uint8_t, WAL_RECORD_SIZE>& out, std::size_t offset, std::uint32_t value) {
    for (int i = 0; i < 4; ++i) {
        out[offset + static_cast<std::size_t>(i)] = static_cast<std::uint8_t>((value >> (i * 8)) & 0xff);
    }
}

std::uint64_t read_u64_from_bytes(const std::array<std::uint8_t, WAL_RECORD_SIZE>& in, std::size_t offset) {
    std::uint64_t value = 0;
    for (int i = 0; i < 8; ++i) {
        value |= static_cast<std::uint64_t>(in[offset + static_cast<std::size_t>(i)]) << (i * 8);
    }
    return value;
}

std::uint32_t read_u32_from_bytes(const std::array<std::uint8_t, WAL_RECORD_SIZE>& in, std::size_t offset) {
    std::uint32_t value = 0;
    for (int i = 0; i < 4; ++i) {
        value |= static_cast<std::uint32_t>(in[offset + static_cast<std::size_t>(i)]) << (i * 8);
    }
    return value;
}

std::string upper_ascii(std::string value) {
    for (char& c : value) {
        c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    }
    return value;
}

template <class... Ts> struct Overloaded : Ts... {
    using Ts::operator()...;
};

template <class... Ts> Overloaded(Ts...) -> Overloaded<Ts...>;

std::vector<std::string> column_names(const Schema& schema) {
    std::vector<std::string> names;
    names.reserve(schema.columns.size());
    for (const auto& column : schema.columns) {
        names.push_back(column.name);
    }
    return names;
}

void require_primary_key_filter(const Table& table, const sql::Filter& filter) {
    const auto& schema = table.schema();
    require(schema.primary_key < schema.columns.size(), "primary key index out of bounds");
    const auto& primary_key = schema.columns.at(schema.primary_key);
    require(filter.column == primary_key.name, "only primary-key WHERE filters are supported");
}

std::vector<std::vector<Value>> scan_between_inclusive(Table& table, std::int32_t start, std::int32_t end) {
    if (start > end) {
        return {};
    }

    if (end == std::numeric_limits<std::int32_t>::max()) {
        auto rows = table.scan(start, end);
        if (auto final_row = table.get_row(end)) {
            rows.push_back(*final_row);
        }
        return rows;
    }

    return table.scan(start, end + 1);
}

} // namespace

Page serialize_node(bool is_leaf, const std::vector<std::int32_t>& keys,
                    const std::vector<std::vector<std::uint8_t>>& values, const std::vector<std::uint32_t>& children) {
    require(keys.size() == values.size(), "node keys and values must have the same length");
    require(keys.size() <= std::numeric_limits<std::uint16_t>::max(), "too many keys in node");
    if (is_leaf) {
        require(children.empty(), "leaf node cannot have children");
    } else {
        require(children.size() == keys.size() + 1, "internal node must have key_count + 1 children");
    }

    Page page{};
    std::size_t offset = PAGE_HEADER_SIZE;
    ensure_capacity(offset, 1);
    page[offset++] = is_leaf ? 1 : 0;
    write_u16(page, offset, static_cast<std::uint16_t>(keys.size()));

    for (std::int32_t key : keys) {
        write_i32(page, offset, key);
    }

    for (const auto& value : values) {
        ensure_capacity(offset, VALUE_SIZE);
        const auto len = static_cast<std::uint16_t>(std::min<std::size_t>(value.size(), VALUE_SIZE - 2));
        page[offset++] = static_cast<std::uint8_t>(len & 0xff);
        page[offset++] = static_cast<std::uint8_t>((len >> 8) & 0xff);
        std::copy_n(value.begin(), len, page.begin() + static_cast<std::ptrdiff_t>(offset));
        offset += VALUE_SIZE - 2;
    }

    for (std::uint32_t child : children) {
        write_u32(page, offset, child);
    }

    return page;
}

Node deserialize_node(const Page& page) {
    std::size_t offset = PAGE_HEADER_SIZE;
    ensure_capacity(offset, 1);
    Node node;
    node.is_leaf = page[offset++] != 0;
    const auto num_keys = static_cast<std::size_t>(read_u16(page, offset));

    node.keys.reserve(num_keys);
    for (std::size_t i = 0; i < num_keys; ++i) {
        node.keys.push_back(read_i32(page, offset));
    }

    node.values.reserve(num_keys);
    for (std::size_t i = 0; i < num_keys; ++i) {
        ensure_capacity(offset, VALUE_SIZE);
        const std::uint16_t len =
            static_cast<std::uint16_t>(page[offset]) | (static_cast<std::uint16_t>(page[offset + 1]) << 8);
        require(len <= VALUE_SIZE - 2, "corrupt node value length");
        std::vector<std::uint8_t> value(page.begin() + static_cast<std::ptrdiff_t>(offset + 2),
                                        page.begin() + static_cast<std::ptrdiff_t>(offset + 2 + len));
        node.values.push_back(std::move(value));
        offset += VALUE_SIZE;
    }

    const auto num_children = node.is_leaf ? 0 : num_keys + 1;
    node.children.reserve(num_children);
    for (std::size_t i = 0; i < num_children; ++i) {
        node.children.push_back(read_u32(page, offset));
    }

    return node;
}

std::uint64_t get_page_lsn(const Page& page) {
    std::uint64_t value = 0;
    for (int i = 0; i < 8; ++i) {
        value |= static_cast<std::uint64_t>(page[static_cast<std::size_t>(i)]) << (i * 8);
    }
    return value;
}

void set_page_lsn(Page& page, std::uint64_t lsn) {
    write_u64(page, 0, lsn);
}

DiskManager::DiskManager(std::string filename) : filename_(std::move(filename)) {
    if (!std::filesystem::exists(filename_)) {
        std::ofstream create(filename_, std::ios::binary);
        require(create.good(), "failed to create database file: " + filename_);
    }

    file_.open(filename_, std::ios::binary | std::ios::in | std::ios::out);
    require(file_.is_open(), "failed to open database file: " + filename_);
}

DiskManager::DiskManager(DiskManager&& other) noexcept = default;

DiskManager& DiskManager::operator=(DiskManager&& other) noexcept = default;

Page DiskManager::read_page(std::uint32_t page_id) {
    Page data{};
    const auto offset = static_cast<std::streamoff>(static_cast<std::uint64_t>(page_id) * PAGE_SIZE);
    file_.clear();
    file_.seekg(offset, std::ios::beg);
    file_.read(reinterpret_cast<char*>(data.data()), static_cast<std::streamsize>(data.size()));
    if (file_.gcount() != static_cast<std::streamsize>(data.size())) {
        file_.clear();
        return Page{};
    }
    return data;
}

void DiskManager::write_page(std::uint32_t page_id, const Page& data) {
    const auto offset = static_cast<std::streamoff>(static_cast<std::uint64_t>(page_id) * PAGE_SIZE);
    file_.clear();
    file_.seekp(offset, std::ios::beg);
    file_.write(reinterpret_cast<const char*>(data.data()), static_cast<std::streamsize>(data.size()));
    require(file_.good(), "failed to write database page");
    file_.flush();
}

void DiskManager::flush() {
    file_.flush();
}

std::array<std::uint8_t, WAL_RECORD_SIZE> WalRecord::serialize() const {
    std::array<std::uint8_t, WAL_RECORD_SIZE> out{};
    write_u64_to_bytes(out, 0, lsn);
    write_u32_to_bytes(out, 8, page_id);
    std::copy(page_data.begin(), page_data.end(), out.begin() + 12);
    return out;
}

WalRecord WalRecord::deserialize(const std::array<std::uint8_t, WAL_RECORD_SIZE>& bytes) {
    WalRecord record;
    record.lsn = read_u64_from_bytes(bytes, 0);
    record.page_id = read_u32_from_bytes(bytes, 8);
    std::copy(bytes.begin() + 12, bytes.end(), record.page_data.begin());
    return record;
}

Wal::Wal(std::string filename) : filename_(std::move(filename)) {
    if (!std::filesystem::exists(filename_)) {
        std::ofstream create(filename_, std::ios::binary);
        require(create.good(), "failed to create WAL file: " + filename_);
    }

    file_.open(filename_, std::ios::binary | std::ios::in | std::ios::out);
    require(file_.is_open(), "failed to open WAL file: " + filename_);

    const auto len = std::filesystem::file_size(filename_);
    current_lsn_ = static_cast<std::uint64_t>(len / WAL_RECORD_SIZE);
    flushed_lsn_ = current_lsn_;
}

Wal::Wal(Wal&& other) noexcept = default;

Wal& Wal::operator=(Wal&& other) noexcept = default;

std::uint64_t Wal::log_page(std::uint32_t page_id, const Page& page_data) {
    const std::uint64_t lsn = current_lsn_++;
    Page logged_page = page_data;
    set_page_lsn(logged_page, lsn);

    WalRecord record{lsn, page_id, logged_page};
    const auto bytes = record.serialize();
    const auto offset = static_cast<std::streamoff>(lsn * WAL_RECORD_SIZE);

    file_.clear();
    file_.seekp(offset, std::ios::beg);
    file_.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    require(file_.good(), "failed to write WAL record");
    file_.flush();
    return lsn;
}

void Wal::flush() {
    file_.flush();
    flushed_lsn_ = current_lsn_;
}

void Wal::flush_to(std::uint64_t target_lsn) {
    if (flushed_lsn_ <= target_lsn) {
        flush();
    }
}

std::uint64_t Wal::flushed_lsn() const {
    return flushed_lsn_;
}

std::uint64_t Wal::current_lsn() const {
    return current_lsn_;
}

std::vector<WalRecord> Wal::read_from(std::uint64_t start_lsn) {
    std::vector<WalRecord> records;
    std::uint64_t lsn = start_lsn;

    while (true) {
        const auto offset = static_cast<std::streamoff>(lsn * WAL_RECORD_SIZE);
        std::array<std::uint8_t, WAL_RECORD_SIZE> bytes{};
        file_.clear();
        file_.seekg(offset, std::ios::beg);
        file_.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
        if (file_.gcount() != static_cast<std::streamsize>(bytes.size())) {
            file_.clear();
            break;
        }

        records.push_back(WalRecord::deserialize(bytes));
        ++lsn;
    }

    return records;
}

void Wal::recover(Wal& wal, DiskManager& disk, std::uint64_t checkpoint_lsn) {
    for (const auto& record : wal.read_from(checkpoint_lsn)) {
        Page page = disk.read_page(record.page_id);
        if (get_page_lsn(page) <= record.lsn) {
            disk.write_page(record.page_id, record.page_data);
        }
    }
}

std::uint64_t Wal::checkpoint(BufferPool& pool) {
    pool.flush();
    return pool.current_lsn();
}

void Wal::write_checkpoint_lsn(const std::string& filename, std::uint64_t lsn) {
    std::ofstream out(filename, std::ios::binary | std::ios::trunc);
    require(out.is_open(), "failed to write checkpoint file");
    for (int i = 0; i < 8; ++i) {
        out.put(static_cast<char>((lsn >> (i * 8)) & 0xff));
    }
    out.flush();
}

std::uint64_t Wal::read_checkpoint_lsn(const std::string& filename) {
    std::ifstream in(filename, std::ios::binary);
    if (!in.is_open()) {
        return 0;
    }
    std::array<std::uint8_t, 8> bytes{};
    in.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    if (in.gcount() != static_cast<std::streamsize>(bytes.size())) {
        return 0;
    }

    std::uint64_t value = 0;
    for (int i = 0; i < 8; ++i) {
        value |= static_cast<std::uint64_t>(bytes[static_cast<std::size_t>(i)]) << (i * 8);
    }
    return value;
}

BufferPool::BufferPool(DiskManager disk, Wal wal, std::size_t capacity)
    : disk_(std::move(disk)), wal_(std::move(wal)), capacity_(capacity) {
    require(capacity_ > 0, "buffer pool capacity must be positive");
}

const Page& BufferPool::get_page(std::uint32_t page_id) {
    if (!pages_.contains(page_id)) {
        while (pages_.size() >= capacity_) {
            evict();
        }
        pages_.emplace(page_id, disk_.read_page(page_id));
    }

    touch(page_id);
    return pages_.at(page_id);
}

void BufferPool::write_page(std::uint32_t page_id, Page data) {
    if (!pages_.contains(page_id)) {
        while (pages_.size() >= capacity_) {
            evict();
        }
    }

    const std::uint64_t lsn = wal_.log_page(page_id, data);
    set_page_lsn(data, lsn);
    pages_[page_id] = data;
    dirty_[page_id] = true;
    page_lsn_[page_id] = lsn;
    touch(page_id);
}

void BufferPool::flush() {
    wal_.flush();
    std::vector<std::uint32_t> dirty_ids;
    dirty_ids.reserve(dirty_.size());
    for (const auto& [page_id, is_dirty] : dirty_) {
        if (is_dirty) {
            dirty_ids.push_back(page_id);
        }
    }

    for (std::uint32_t page_id : dirty_ids) {
        disk_.write_page(page_id, pages_.at(page_id));
    }
    disk_.flush();
    dirty_.clear();
}

std::uint64_t BufferPool::current_lsn() const {
    return wal_.current_lsn();
}

std::size_t BufferPool::cached_page_count() const {
    return pages_.size();
}

bool BufferPool::contains_page(std::uint32_t page_id) const {
    return pages_.contains(page_id);
}

bool BufferPool::dirty_empty() const {
    return dirty_.empty();
}

void BufferPool::touch(std::uint32_t page_id) {
    auto found = std::find(lru_order_.begin(), lru_order_.end(), page_id);
    if (found != lru_order_.end()) {
        lru_order_.erase(found);
    }
    lru_order_.push_back(page_id);
}

void BufferPool::evict() {
    require(!lru_order_.empty(), "cannot evict from empty buffer pool");
    const std::uint32_t victim_id = lru_order_.front();
    if (dirty_.contains(victim_id) && dirty_.at(victim_id)) {
        const std::uint64_t page_lsn = page_lsn_.contains(victim_id) ? page_lsn_.at(victim_id) : 0;
        wal_.flush_to(page_lsn);
        disk_.write_page(victim_id, pages_.at(victim_id));
        dirty_.erase(victim_id);
    }

    pages_.erase(victim_id);
    page_lsn_.erase(victim_id);
    lru_order_.erase(lru_order_.begin());
}

DataType DataType::Int() {
    return {DataTypeKind::Int, 0};
}

DataType DataType::Varchar(std::size_t max_length) {
    return {DataTypeKind::Varchar, max_length};
}

DataType DataType::Bool() {
    return {DataTypeKind::Bool, 0};
}

bool DataType::operator==(const DataType& other) const {
    return kind == other.kind && max_length == other.max_length;
}

Value Value::Int(std::int32_t value) {
    return Value{value};
}

Value Value::Varchar(std::string value) {
    return Value{std::move(value)};
}

Value Value::Bool(bool value) {
    return Value{value};
}

Value Value::Null() {
    return Value{std::monostate{}};
}

bool Value::is_null() const {
    return std::holds_alternative<std::monostate>(data);
}

bool Value::is_int() const {
    return std::holds_alternative<std::int32_t>(data);
}

std::int32_t Value::as_int() const {
    if (!is_int()) {
        fail("value is not an int");
    }
    return std::get<std::int32_t>(data);
}

std::string Value::to_string() const {
    if (std::holds_alternative<std::monostate>(data)) {
        return "NULL";
    }
    if (std::holds_alternative<std::int32_t>(data)) {
        return std::to_string(std::get<std::int32_t>(data));
    }
    if (std::holds_alternative<std::string>(data)) {
        return std::get<std::string>(data);
    }
    return std::get<bool>(data) ? "true" : "false";
}

bool Value::operator==(const Value& other) const {
    return data == other.data;
}

std::vector<std::uint8_t> Schema::encode_row(const std::vector<Value>& row) const {
    require(row.size() == columns.size(), "row value count does not match schema");
    std::vector<std::uint8_t> out;

    for (std::size_t i = 0; i < columns.size(); ++i) {
        const auto& column = columns[i];
        const auto& value = row[i];

        switch (column.data_type.kind) {
        case DataTypeKind::Int: {
            require(value.is_int(), "type mismatch: expected int");
            const auto raw = static_cast<std::uint32_t>(value.as_int());
            for (int b = 0; b < 4; ++b) {
                out.push_back(static_cast<std::uint8_t>((raw >> (b * 8)) & 0xff));
            }
            break;
        }
        case DataTypeKind::Varchar: {
            require(std::holds_alternative<std::string>(value.data), "type mismatch: expected varchar");
            const auto& text = std::get<std::string>(value.data);
            const auto len = std::min<std::size_t>(text.size(), column.data_type.max_length);
            out.insert(out.end(), text.begin(), text.begin() + static_cast<std::ptrdiff_t>(len));
            out.insert(out.end(), column.data_type.max_length - len, 0);
            break;
        }
        case DataTypeKind::Bool: {
            require(std::holds_alternative<bool>(value.data), "type mismatch: expected bool");
            out.push_back(std::get<bool>(value.data) ? 1 : 0);
            break;
        }
        }
    }

    return out;
}

std::vector<Value> Schema::decode_row(const std::vector<std::uint8_t>& data) const {
    std::vector<Value> row;
    std::size_t offset = 0;

    for (const auto& column : columns) {
        switch (column.data_type.kind) {
        case DataTypeKind::Int: {
            require(offset + 4 <= data.size(), "encoded row is truncated");
            std::uint32_t raw = 0;
            for (int b = 0; b < 4; ++b) {
                raw |= static_cast<std::uint32_t>(data[offset + static_cast<std::size_t>(b)]) << (b * 8);
            }
            row.push_back(Value::Int(static_cast<std::int32_t>(raw)));
            offset += 4;
            break;
        }
        case DataTypeKind::Varchar: {
            const std::size_t max_len = column.data_type.max_length;
            require(offset + max_len <= data.size(), "encoded row is truncated");
            std::size_t end = 0;
            while (end < max_len && data[offset + end] != 0) {
                ++end;
            }
            row.push_back(Value::Varchar(std::string(data.begin() + static_cast<std::ptrdiff_t>(offset),
                                                     data.begin() + static_cast<std::ptrdiff_t>(offset + end))));
            offset += max_len;
            break;
        }
        case DataTypeKind::Bool: {
            require(offset + 1 <= data.size(), "encoded row is truncated");
            row.push_back(Value::Bool(data[offset] != 0));
            ++offset;
            break;
        }
        }
    }

    return row;
}

DiskBtree::DiskBtree(std::shared_ptr<BufferPool> pool, std::size_t degree)
    : pool_(std::move(pool)), root_page_id_(0), next_page_id_(1), degree_(degree) {
    require(degree_ >= 2, "B-tree degree must be at least 2");
    pool_->write_page(root_page_id_, serialize_node(true, {}, {}, {}));
}

DiskBtree::DiskBtree(std::shared_ptr<BufferPool> pool, std::uint32_t root_page_id, std::uint32_t next_page_id,
                     std::size_t degree)
    : pool_(std::move(pool)), root_page_id_(root_page_id), next_page_id_(next_page_id), degree_(degree) {
    require(degree_ >= 2, "B-tree degree must be at least 2");
}

std::uint32_t DiskBtree::next_page_id() const {
    return next_page_id_;
}

std::uint32_t DiskBtree::root_page_id() const {
    return root_page_id_;
}

std::optional<std::vector<std::uint8_t>> DiskBtree::search(std::int32_t key) {
    return search_node(root_page_id_, key);
}

void DiskBtree::insert(std::int32_t key, std::vector<std::uint8_t> value) {
    Page page = pool_->get_page(root_page_id_);
    Node root = deserialize_node(page);
    const std::size_t max_keys = 2 * degree_ - 1;

    if (root.keys.size() == max_keys) {
        const std::uint32_t new_root_id = allocate_page();
        pool_->write_page(new_root_id, serialize_node(false, {}, {}, {root_page_id_}));
        root_page_id_ = new_root_id;
        split_child(new_root_id, 0);
        insert_non_full(new_root_id, key, std::move(value));
    } else {
        insert_non_full(root_page_id_, key, std::move(value));
    }
}

void DiskBtree::erase(std::int32_t key) {
    delete_key(root_page_id_, key);
    Page page = pool_->get_page(root_page_id_);
    Node root = deserialize_node(page);
    if (root.keys.empty() && !root.is_leaf) {
        root_page_id_ = root.children.at(0);
    }
}

std::vector<std::pair<std::int32_t, std::vector<std::uint8_t>>> DiskBtree::scan(std::int32_t start, std::int32_t end) {
    std::vector<std::pair<std::int32_t, std::vector<std::uint8_t>>> out;
    if (start >= end) {
        return out;
    }
    scan_node(root_page_id_, start, end, out);
    return out;
}

std::vector<std::pair<std::int32_t, std::vector<std::uint8_t>>> DiskBtree::scan_all() {
    return scan(std::numeric_limits<std::int32_t>::min(), std::numeric_limits<std::int32_t>::max());
}

void DiskBtree::flush() {
    pool_->flush();
}

std::optional<std::vector<std::uint8_t>> DiskBtree::search_node(std::uint32_t page_id, std::int32_t key) {
    Node node = deserialize_node(pool_->get_page(page_id));
    auto it = std::lower_bound(node.keys.begin(), node.keys.end(), key);
    const auto idx = static_cast<std::size_t>(it - node.keys.begin());

    if (it != node.keys.end() && *it == key) {
        return node.values.at(idx);
    }
    if (!node.is_leaf) {
        return search_node(node.children.at(idx), key);
    }
    return std::nullopt;
}

void DiskBtree::insert_non_full(std::uint32_t page_id, std::int32_t key, std::vector<std::uint8_t> value) {
    Node node = deserialize_node(pool_->get_page(page_id));
    auto it = std::lower_bound(node.keys.begin(), node.keys.end(), key);
    std::size_t pos = static_cast<std::size_t>(it - node.keys.begin());

    if (it != node.keys.end() && *it == key) {
        node.values[pos] = std::move(value);
        pool_->write_page(page_id, serialize_node(node.is_leaf, node.keys, node.values, node.children));
        return;
    }

    if (node.is_leaf) {
        node.keys.insert(node.keys.begin() + static_cast<std::ptrdiff_t>(pos), key);
        node.values.insert(node.values.begin() + static_cast<std::ptrdiff_t>(pos), std::move(value));
        pool_->write_page(page_id, serialize_node(true, node.keys, node.values, node.children));
        return;
    }

    std::uint32_t child_id = node.children.at(pos);
    Node child = deserialize_node(pool_->get_page(child_id));
    const std::size_t max_keys = 2 * degree_ - 1;

    if (child.keys.size() == max_keys) {
        split_child(page_id, pos);
        node = deserialize_node(pool_->get_page(page_id));
        if (key == node.keys.at(pos)) {
            node.values[pos] = std::move(value);
            pool_->write_page(page_id, serialize_node(node.is_leaf, node.keys, node.values, node.children));
            return;
        }
        child_id = key > node.keys.at(pos) ? node.children.at(pos + 1) : node.children.at(pos);
    }

    insert_non_full(child_id, key, std::move(value));
}

void DiskBtree::split_child(std::uint32_t parent_id, std::size_t idx) {
    Node parent = deserialize_node(pool_->get_page(parent_id));
    const std::uint32_t child_id = parent.children.at(idx);
    Node child = deserialize_node(pool_->get_page(child_id));
    const std::size_t mid = degree_ - 1;

    std::vector<std::int32_t> left_keys(child.keys.begin(), child.keys.begin() + static_cast<std::ptrdiff_t>(mid));
    std::vector<std::vector<std::uint8_t>> left_values(child.values.begin(),
                                                       child.values.begin() + static_cast<std::ptrdiff_t>(mid));
    std::vector<std::int32_t> right_keys(child.keys.begin() + static_cast<std::ptrdiff_t>(mid + 1), child.keys.end());
    std::vector<std::vector<std::uint8_t>> right_values(child.values.begin() + static_cast<std::ptrdiff_t>(mid + 1),
                                                        child.values.end());

    std::vector<std::uint32_t> left_children;
    std::vector<std::uint32_t> right_children;
    if (!child.is_leaf) {
        left_children.assign(child.children.begin(), child.children.begin() + static_cast<std::ptrdiff_t>(mid + 1));
        right_children.assign(child.children.begin() + static_cast<std::ptrdiff_t>(mid + 1), child.children.end());
    }

    const std::int32_t median_key = child.keys.at(mid);
    std::vector<std::uint8_t> median_value = child.values.at(mid);

    pool_->write_page(child_id, serialize_node(child.is_leaf, left_keys, left_values, left_children));

    const std::uint32_t right_id = allocate_page();
    pool_->write_page(right_id, serialize_node(child.is_leaf, right_keys, right_values, right_children));

    parent.keys.insert(parent.keys.begin() + static_cast<std::ptrdiff_t>(idx), median_key);
    parent.values.insert(parent.values.begin() + static_cast<std::ptrdiff_t>(idx), std::move(median_value));
    parent.children.insert(parent.children.begin() + static_cast<std::ptrdiff_t>(idx + 1), right_id);
    pool_->write_page(parent_id, serialize_node(parent.is_leaf, parent.keys, parent.values, parent.children));
}

void DiskBtree::delete_key(std::uint32_t page_id, std::int32_t key) {
    Node node = deserialize_node(pool_->get_page(page_id));
    auto it = std::lower_bound(node.keys.begin(), node.keys.end(), key);
    std::size_t idx = static_cast<std::size_t>(it - node.keys.begin());

    if (idx < node.keys.size() && node.keys[idx] == key) {
        if (node.is_leaf) {
            node.keys.erase(node.keys.begin() + static_cast<std::ptrdiff_t>(idx));
            node.values.erase(node.values.begin() + static_cast<std::ptrdiff_t>(idx));
            pool_->write_page(page_id, serialize_node(true, node.keys, node.values, node.children));
            return;
        }

        const std::uint32_t left_id = node.children.at(idx);
        const std::uint32_t right_id = node.children.at(idx + 1);
        Node left = deserialize_node(pool_->get_page(left_id));
        Node right = deserialize_node(pool_->get_page(right_id));

        if (left.keys.size() >= degree_) {
            auto [pred_key, pred_value] = get_predecessor(left_id);
            node.keys[idx] = pred_key;
            node.values[idx] = pred_value;
            pool_->write_page(page_id, serialize_node(node.is_leaf, node.keys, node.values, node.children));
            delete_key(left_id, pred_key);
        } else if (right.keys.size() >= degree_) {
            auto [succ_key, succ_value] = get_successor(right_id);
            node.keys[idx] = succ_key;
            node.values[idx] = succ_value;
            pool_->write_page(page_id, serialize_node(node.is_leaf, node.keys, node.values, node.children));
            delete_key(right_id, succ_key);
        } else {
            merge(page_id, idx);
            delete_key(left_id, key);
        }
        return;
    }

    if (node.is_leaf) {
        return;
    }

    const bool was_last_child = idx == node.keys.size();
    const std::uint32_t child_id = node.children.at(idx);
    Node child = deserialize_node(pool_->get_page(child_id));

    if (child.keys.size() < degree_) {
        fill(page_id, idx);
    }

    node = deserialize_node(pool_->get_page(page_id));
    if (was_last_child && idx >= node.children.size()) {
        delete_key(node.children.at(idx - 1), key);
    } else {
        delete_key(node.children.at(idx), key);
    }
}

std::pair<std::int32_t, std::vector<std::uint8_t>> DiskBtree::get_predecessor(std::uint32_t page_id) {
    Node node = deserialize_node(pool_->get_page(page_id));
    if (node.is_leaf) {
        const std::size_t last = node.keys.size() - 1;
        return {node.keys.at(last), node.values.at(last)};
    }
    return get_predecessor(node.children.back());
}

std::pair<std::int32_t, std::vector<std::uint8_t>> DiskBtree::get_successor(std::uint32_t page_id) {
    Node node = deserialize_node(pool_->get_page(page_id));
    if (node.is_leaf) {
        return {node.keys.at(0), node.values.at(0)};
    }
    return get_successor(node.children.front());
}

void DiskBtree::merge(std::uint32_t parent_id, std::size_t idx) {
    Node parent = deserialize_node(pool_->get_page(parent_id));
    const std::uint32_t left_id = parent.children.at(idx);
    const std::uint32_t right_id = parent.children.at(idx + 1);
    Node left = deserialize_node(pool_->get_page(left_id));
    Node right = deserialize_node(pool_->get_page(right_id));

    left.keys.push_back(parent.keys.at(idx));
    left.values.push_back(parent.values.at(idx));
    left.keys.insert(left.keys.end(), right.keys.begin(), right.keys.end());
    left.values.insert(left.values.end(), right.values.begin(), right.values.end());
    left.children.insert(left.children.end(), right.children.begin(), right.children.end());

    parent.keys.erase(parent.keys.begin() + static_cast<std::ptrdiff_t>(idx));
    parent.values.erase(parent.values.begin() + static_cast<std::ptrdiff_t>(idx));
    parent.children.erase(parent.children.begin() + static_cast<std::ptrdiff_t>(idx + 1));

    pool_->write_page(left_id, serialize_node(left.is_leaf, left.keys, left.values, left.children));
    pool_->write_page(parent_id, serialize_node(parent.is_leaf, parent.keys, parent.values, parent.children));
}

void DiskBtree::fill(std::uint32_t parent_id, std::size_t idx) {
    Node parent = deserialize_node(pool_->get_page(parent_id));

    if (idx > 0) {
        Node left = deserialize_node(pool_->get_page(parent.children.at(idx - 1)));
        if (left.keys.size() >= degree_) {
            borrow_from_prev(parent_id, idx);
            return;
        }
    }

    if (idx + 1 < parent.children.size()) {
        Node right = deserialize_node(pool_->get_page(parent.children.at(idx + 1)));
        if (right.keys.size() >= degree_) {
            borrow_from_next(parent_id, idx);
            return;
        }
    }

    if (idx + 1 < parent.children.size()) {
        merge(parent_id, idx);
    } else {
        merge(parent_id, idx - 1);
    }
}

void DiskBtree::borrow_from_prev(std::uint32_t parent_id, std::size_t idx) {
    Node parent = deserialize_node(pool_->get_page(parent_id));
    const std::uint32_t child_id = parent.children.at(idx);
    const std::uint32_t sibling_id = parent.children.at(idx - 1);
    Node child = deserialize_node(pool_->get_page(child_id));
    Node sibling = deserialize_node(pool_->get_page(sibling_id));

    child.keys.insert(child.keys.begin(), parent.keys.at(idx - 1));
    child.values.insert(child.values.begin(), parent.values.at(idx - 1));
    parent.keys[idx - 1] = sibling.keys.back();
    parent.values[idx - 1] = sibling.values.back();
    sibling.keys.pop_back();
    sibling.values.pop_back();

    if (!sibling.is_leaf) {
        child.children.insert(child.children.begin(), sibling.children.back());
        sibling.children.pop_back();
    }

    pool_->write_page(parent_id, serialize_node(parent.is_leaf, parent.keys, parent.values, parent.children));
    pool_->write_page(child_id, serialize_node(child.is_leaf, child.keys, child.values, child.children));
    pool_->write_page(sibling_id, serialize_node(sibling.is_leaf, sibling.keys, sibling.values, sibling.children));
}

void DiskBtree::borrow_from_next(std::uint32_t parent_id, std::size_t idx) {
    Node parent = deserialize_node(pool_->get_page(parent_id));
    const std::uint32_t child_id = parent.children.at(idx);
    const std::uint32_t sibling_id = parent.children.at(idx + 1);
    Node child = deserialize_node(pool_->get_page(child_id));
    Node sibling = deserialize_node(pool_->get_page(sibling_id));

    child.keys.push_back(parent.keys.at(idx));
    child.values.push_back(parent.values.at(idx));
    parent.keys[idx] = sibling.keys.front();
    parent.values[idx] = sibling.values.front();
    sibling.keys.erase(sibling.keys.begin());
    sibling.values.erase(sibling.values.begin());

    if (!sibling.is_leaf) {
        child.children.push_back(sibling.children.front());
        sibling.children.erase(sibling.children.begin());
    }

    pool_->write_page(parent_id, serialize_node(parent.is_leaf, parent.keys, parent.values, parent.children));
    pool_->write_page(child_id, serialize_node(child.is_leaf, child.keys, child.values, child.children));
    pool_->write_page(sibling_id, serialize_node(sibling.is_leaf, sibling.keys, sibling.values, sibling.children));
}

void DiskBtree::scan_node(std::uint32_t page_id, std::int32_t start, std::int32_t end,
                          std::vector<std::pair<std::int32_t, std::vector<std::uint8_t>>>& out) {
    Node node = deserialize_node(pool_->get_page(page_id));

    if (node.is_leaf) {
        for (std::size_t i = 0; i < node.keys.size(); ++i) {
            if (node.keys[i] >= end) {
                break;
            }
            if (node.keys[i] >= start) {
                out.emplace_back(node.keys[i], node.values[i]);
            }
        }
        return;
    }

    for (std::size_t i = 0; i <= node.keys.size(); ++i) {
        const bool left_ok = i == 0 || node.keys[i - 1] < end;
        const bool right_ok = i == node.keys.size() || node.keys[i] >= start;
        if (left_ok && right_ok) {
            scan_node(node.children.at(i), start, end, out);
        }

        if (i < node.keys.size()) {
            if (node.keys[i] >= end) {
                return;
            }
            if (node.keys[i] >= start) {
                out.emplace_back(node.keys[i], node.values[i]);
            }
        }
    }
}

std::uint32_t DiskBtree::allocate_page() {
    return next_page_id_++;
}

Table::Table(std::string name, Schema schema, std::shared_ptr<BufferPool> pool, std::size_t degree)
    : name_(std::move(name)), schema_(std::move(schema)), tree_(std::move(pool), degree) {}

Table::Table(std::string name, Schema schema, std::shared_ptr<BufferPool> pool, std::uint32_t root_page_id,
             std::uint32_t next_page_id, std::size_t degree)
    : name_(std::move(name)), schema_(std::move(schema)), tree_(std::move(pool), root_page_id, next_page_id, degree) {}

const std::string& Table::name() const {
    return name_;
}

const Schema& Table::schema() const {
    return schema_;
}

void Table::insert_row(const std::vector<Value>& row) {
    require(schema_.primary_key < row.size(), "primary key index out of bounds");
    const Value& pk = row[schema_.primary_key];
    require(pk.is_int(), "primary key must be an int");
    tree_.insert(pk.as_int(), schema_.encode_row(row));
}

std::optional<std::vector<Value>> Table::get_row(std::int32_t pk) {
    auto encoded = tree_.search(pk);
    if (!encoded) {
        return std::nullopt;
    }
    return schema_.decode_row(*encoded);
}

void Table::delete_row(std::int32_t pk) {
    tree_.erase(pk);
}

std::vector<std::vector<Value>> Table::scan(std::int32_t start, std::int32_t end) {
    std::vector<std::vector<Value>> rows;
    for (auto& [key, encoded] : tree_.scan(start, end)) {
        (void)key;
        rows.push_back(schema_.decode_row(encoded));
    }
    return rows;
}

std::vector<std::vector<Value>> Table::scan_all() {
    std::vector<std::vector<Value>> rows;
    for (auto& [key, encoded] : tree_.scan_all()) {
        (void)key;
        rows.push_back(schema_.decode_row(encoded));
    }
    return rows;
}

std::uint32_t Table::next_page_id() const {
    return tree_.next_page_id();
}

std::uint32_t Table::root_page_id() const {
    return tree_.root_page_id();
}

void Table::flush() {
    tree_.flush();
}

Catalog::Catalog(BufferPool pool) : pool_(std::make_shared<BufferPool>(std::move(pool))) {
    Page page = pool_->get_page(0);
    auto [tables, next_page_id] = deserialize_catalog(page);
    tables_ = std::move(tables);
    next_page_id_ = next_page_id > 0 ? next_page_id : 1;
}

void Catalog::create_table(const std::string& name, Schema schema, std::size_t degree) {
    require(get_table_meta(name) == nullptr, "table already exists: " + name);
    const std::uint32_t root_page_id = next_page_id_++;
    pool_->write_page(root_page_id, serialize_node(true, {}, {}, {}));
    tables_.push_back(TableMeta{name, std::move(schema), root_page_id, degree});
}

const TableMeta* Catalog::get_table_meta(const std::string& name) const {
    auto it = std::find_if(tables_.begin(), tables_.end(), [&](const TableMeta& meta) { return meta.name == name; });
    return it == tables_.end() ? nullptr : &*it;
}

std::optional<Table> Catalog::get_table(const std::string& name) const {
    const TableMeta* meta = get_table_meta(name);
    if (meta == nullptr) {
        return std::nullopt;
    }

    return Table(meta->name, meta->schema, pool_, meta->root_page_id, next_page_id_, meta->degree);
}

void Catalog::update_next_page_id(std::uint32_t id) {
    if (id > next_page_id_) {
        next_page_id_ = id;
    }
}

void Catalog::update_table_storage(const Table& table) {
    for (auto& meta : tables_) {
        if (meta.name == table.name()) {
            meta.root_page_id = table.root_page_id();
            update_next_page_id(table.next_page_id());
            return;
        }
    }
    fail("table not found: " + table.name());
}

void Catalog::flush() {
    pool_->write_page(0, serialize_catalog(tables_, next_page_id_));
    pool_->flush();
}

Page Catalog::serialize_catalog(const std::vector<TableMeta>& tables, std::uint32_t next_page_id) {
    Page page{};
    std::size_t offset = PAGE_HEADER_SIZE;
    write_u32(page, offset, next_page_id);
    write_u16(page, offset, static_cast<std::uint16_t>(tables.size()));

    for (const auto& table : tables) {
        require(table.name.size() <= std::numeric_limits<std::uint16_t>::max(), "table name too long");
        write_u16(page, offset, static_cast<std::uint16_t>(table.name.size()));
        ensure_capacity(offset, table.name.size());
        std::copy(table.name.begin(), table.name.end(), page.begin() + static_cast<std::ptrdiff_t>(offset));
        offset += table.name.size();

        write_u32(page, offset, table.root_page_id);
        write_u32(page, offset, static_cast<std::uint32_t>(table.degree));
        write_u32(page, offset, static_cast<std::uint32_t>(table.schema.primary_key));
        write_u16(page, offset, static_cast<std::uint16_t>(table.schema.columns.size()));

        for (const auto& column : table.schema.columns) {
            require(column.name.size() <= std::numeric_limits<std::uint16_t>::max(), "column name too long");
            write_u16(page, offset, static_cast<std::uint16_t>(column.name.size()));
            ensure_capacity(offset, column.name.size());
            std::copy(column.name.begin(), column.name.end(), page.begin() + static_cast<std::ptrdiff_t>(offset));
            offset += column.name.size();

            ensure_capacity(offset, 1);
            switch (column.data_type.kind) {
            case DataTypeKind::Int:
                page[offset++] = 0;
                write_u32(page, offset, 0);
                break;
            case DataTypeKind::Varchar:
                page[offset++] = 1;
                write_u32(page, offset, static_cast<std::uint32_t>(column.data_type.max_length));
                break;
            case DataTypeKind::Bool:
                page[offset++] = 2;
                write_u32(page, offset, 0);
                break;
            }
        }
    }

    return page;
}

std::pair<std::vector<TableMeta>, std::uint32_t> Catalog::deserialize_catalog(const Page& page) {
    std::size_t offset = PAGE_HEADER_SIZE;
    const std::uint32_t next_page_id = read_u32(page, offset);
    const std::uint16_t num_tables = read_u16(page, offset);
    std::vector<TableMeta> tables;
    tables.reserve(num_tables);

    if (next_page_id == 0 && num_tables == 0) {
        return {tables, 0};
    }

    for (std::uint16_t t = 0; t < num_tables; ++t) {
        const std::size_t name_len = read_u16(page, offset);
        ensure_capacity(offset, name_len);
        std::string name(page.begin() + static_cast<std::ptrdiff_t>(offset),
                         page.begin() + static_cast<std::ptrdiff_t>(offset + name_len));
        offset += name_len;

        const std::uint32_t root_page_id = read_u32(page, offset);
        const std::size_t degree = read_u32(page, offset);
        const std::size_t primary_key = read_u32(page, offset);
        const std::uint16_t num_cols = read_u16(page, offset);

        std::vector<Column> columns;
        columns.reserve(num_cols);
        for (std::uint16_t c = 0; c < num_cols; ++c) {
            const std::size_t col_len = read_u16(page, offset);
            ensure_capacity(offset, col_len);
            std::string col_name(page.begin() + static_cast<std::ptrdiff_t>(offset),
                                 page.begin() + static_cast<std::ptrdiff_t>(offset + col_len));
            offset += col_len;

            ensure_capacity(offset, 1);
            const std::uint8_t tag = page[offset++];
            const std::size_t max_len = read_u32(page, offset);

            DataType data_type;
            if (tag == 0) {
                data_type = DataType::Int();
            } else if (tag == 1) {
                data_type = DataType::Varchar(max_len);
            } else if (tag == 2) {
                data_type = DataType::Bool();
            } else {
                fail("unknown data type tag in catalog");
            }

            columns.push_back(Column{std::move(col_name), data_type});
        }

        tables.push_back(TableMeta{std::move(name), Schema{std::move(columns), primary_key}, root_page_id, degree});
    }

    return {tables, next_page_id};
}

Database::Database(std::string db_file, std::string wal_file, std::size_t buffer_pages,
                   std::size_t default_btree_degree)
    : catalog_(BufferPool(DiskManager(std::move(db_file)), Wal(std::move(wal_file)), buffer_pages)),
      default_btree_degree_(default_btree_degree) {
    require(default_btree_degree >= 2, "default B-tree degree must at least be 2");
};

SqlResult Database::execute(const std::string& source) {
    sql::Statement statement = sql::parse(sql::tokenize(source));

    return std::visit(Overloaded([&](const sql::CreateTable& create) { return execute_create(create); },
                                 [&](const sql::Insert& insert) { return execute_insert(insert); },
                                 [&](const sql::Select& select) { return execute_select(select); }

                                 ),
                      statement);
}

void Database::flush() {
    catalog_.flush();
}

SqlResult Database::execute_create(const sql::CreateTable& statement) {
    std::vector<Column> columns;
    columns.reserve(statement.columns.size());

    for (const auto& column : statement.columns) {
        columns.push_back(Column(column.name, column.data_type));
    }

    catalog_.create_table(statement.name, Schema(std::move(columns), statement.primary_key), default_btree_degree_);

    catalog_.flush();
    return SqlResult();
}

SqlResult Database::execute_insert(const sql::Insert& statement) {
    auto table = catalog_.get_table(statement.table);
    require(table.has_value(), "table not found: " + statement.table);

    table->insert_row(statement.values);
    catalog_.update_table_storage(*table);
    catalog_.flush();

    SqlResult result;
    result.rows_affected = 1;
    return result;
}

SqlResult Database::execute_select(const sql::Select& statement) {
    auto table = catalog_.get_table(statement.table);
    require(table.has_value(), "table not found: " + statement.table);

    SqlResult result;
    result.columns = column_names(table->schema());

    switch (statement.filter.kind) {
    case sql::FilterKind::None:
        result.rows = table->scan_all();
        break;

    case sql::FilterKind::Eq: {
        require_primary_key_filter(*table, statement.filter);
        require(statement.filter.left.is_int(), "primary key equality filter must use an int");

        if (auto row = table->get_row(statement.filter.left.as_int())) {
            result.rows.push_back(*row);
        }
        break;
    }
    case sql::FilterKind::Between: {
        require_primary_key_filter(*table, statement.filter);
        require(statement.filter.left.is_int(), "primary-key range start must use an int");
        require(statement.filter.right.is_int(), "primary-key range end must use an int");
        result.rows = scan_between_inclusive(*table, statement.filter.left.as_int(), statement.filter.right.as_int());
        break;
    }

    default:
        break;
    }
    return result;
}
namespace sql {
bool Token::operator==(TokenKind expected) const {
    return kind == expected;
}

std::vector<Token> tokenize(const std::string& source) {
    std::vector<Token> tokens;
    std::size_t i = 0;

    auto push = [&](TokenKind kind) { tokens.push_back(Token{kind, {}, 0}); };

    while (i < source.size()) {
        const char ch = source[i];
        if (std::isspace(static_cast<unsigned char>(ch))) {
            ++i;
            continue;
        }

        switch (ch) {
        case '(':
            push(TokenKind::LParen);
            ++i;
            continue;
        case ')':
            push(TokenKind::RParen);
            ++i;
            continue;
        case ',':
            push(TokenKind::Comma);
            ++i;
            continue;
        case ';':
            push(TokenKind::Semicolon);
            ++i;
            continue;
        case '*':
            push(TokenKind::Star);
            ++i;
            continue;
        case '=':
            push(TokenKind::Eq);
            ++i;
            continue;
        case '\'': {
            ++i;
            std::string text;
            while (i < source.size() && source[i] != '\'') {
                text.push_back(source[i++]);
            }
            require(i < source.size(), "unterminated string literal");
            ++i;
            tokens.push_back(Token{TokenKind::StrLit, std::move(text), 0});
            continue;
        }
        default:
            break;
        }

        const bool negative_int =
            ch == '-' && i + 1 < source.size() && std::isdigit(static_cast<unsigned char>(source[i + 1]));
        if (std::isdigit(static_cast<unsigned char>(ch)) || negative_int) {
            const std::size_t start = i;
            if (source[i] == '-') {
                ++i;
            }
            while (i < source.size() && std::isdigit(static_cast<unsigned char>(source[i]))) {
                ++i;
            }
            const std::string text = source.substr(start, i - start);
            tokens.push_back(Token{TokenKind::IntLit, text, std::stoi(text)});
            continue;
        }

        if (std::isalpha(static_cast<unsigned char>(ch)) || ch == '_') {
            const std::size_t start = i;
            while (i < source.size() && (std::isalnum(static_cast<unsigned char>(source[i])) || source[i] == '_')) {
                ++i;
            }

            std::string text = source.substr(start, i - start);
            const std::string keyword = upper_ascii(text);
            static const std::unordered_map<std::string, TokenKind> keywords = {
                {"CREATE", TokenKind::Create}, {"TABLE", TokenKind::Table},     {"INSERT", TokenKind::Insert},
                {"INTO", TokenKind::Into},     {"VALUES", TokenKind::Values},   {"SELECT", TokenKind::Select},
                {"FROM", TokenKind::From},     {"WHERE", TokenKind::Where},     {"BETWEEN", TokenKind::Between},
                {"AND", TokenKind::And},       {"INT", TokenKind::Int},         {"VARCHAR", TokenKind::Varchar},
                {"BOOL", TokenKind::Bool},     {"PRIMARY", TokenKind::Primary}, {"KEY", TokenKind::Key},
                {"TRUE", TokenKind::True},     {"FALSE", TokenKind::False},
            };

            auto found = keywords.find(keyword);
            if (found != keywords.end()) {
                tokens.push_back(Token{found->second, {}, 0});
            } else {
                tokens.push_back(Token{TokenKind::Ident, std::move(text), 0});
            }
            continue;
        }

        fail("unexpected character in SQL: " + std::string(1, ch));
    }

    tokens.push_back(Token{TokenKind::Eof, {}, 0});
    return tokens;
}

class Parser {
  public:
    explicit Parser(std::vector<Token> tokens) : tokens_(std::move(tokens)) {}

    Statement parse() {
        Statement statement = parse_statement();
        if (peek().kind == TokenKind::Semicolon) {
            advance();
        }
        if (peek().kind != TokenKind::Eof) {
            fail("unexpected trailing SQL token");
        }
        return statement;
    }

  private:
    const Token& peek() const {
        require(pos_ < tokens_.size(), "parser advanced past end");
        return tokens_[pos_];
    }

    Token advance() {
        Token token = peek();
        if (pos_ + 1 < tokens_.size()) {
            ++pos_;
        }
        return token;
    }

    void expect(TokenKind kind) {
        if (peek().kind != kind) {
            fail("unexpected SQL token");
        }
        advance();
    }

    std::string expect_ident(const std::string& label) {
        if (peek().kind != TokenKind::Ident) {
            fail("expected " + label);
        }
        return advance().text;
    }

    Statement parse_statement() {
        switch (peek().kind) {
        case TokenKind::Create:
            return parse_create();
        case TokenKind::Insert:
            return parse_insert();
        case TokenKind::Select:
            return parse_select();
        default:
            fail("expected SQL statement");
        }
    }

    Statement parse_create() {
        expect(TokenKind::Create);
        expect(TokenKind::Table);
        CreateTable create;
        create.name = expect_ident("table name");
        expect(TokenKind::LParen);

        std::optional<std::size_t> primary_key;
        while (true) {
            ColumnDef column;
            column.name = expect_ident("column name");
            column.data_type = parse_type();

            if (peek().kind == TokenKind::Primary) {
                advance();
                expect(TokenKind::Key);
                if (primary_key.has_value()) {
                    fail("multiple primary keys are not allowed");
                }
                primary_key = create.columns.size();
            }

            create.columns.push_back(std::move(column));

            if (peek().kind == TokenKind::Comma) {
                advance();
                continue;
            }
            expect(TokenKind::RParen);
            break;
        }

        if (!primary_key.has_value()) {
            fail("missing PRIMARY KEY");
        }
        create.primary_key = *primary_key;
        return create;
    }

    DataType parse_type() {
        switch (advance().kind) {
        case TokenKind::Int:
            return DataType::Int();
        case TokenKind::Bool:
            return DataType::Bool();
        case TokenKind::Varchar: {
            expect(TokenKind::LParen);
            if (peek().kind != TokenKind::IntLit) {
                fail("expected varchar length");
            }
            const auto max_len = static_cast<std::size_t>(advance().int_value);
            expect(TokenKind::RParen);
            return DataType::Varchar(max_len);
        }
        default:
            fail("expected column type");
        }
    }

    Statement parse_insert() {
        expect(TokenKind::Insert);
        expect(TokenKind::Into);
        Insert insert;
        insert.table = expect_ident("table name");
        expect(TokenKind::Values);
        expect(TokenKind::LParen);

        while (true) {
            insert.values.push_back(parse_value());
            if (peek().kind == TokenKind::Comma) {
                advance();
                continue;
            }
            expect(TokenKind::RParen);
            break;
        }

        return insert;
    }

    Statement parse_select() {
        expect(TokenKind::Select);
        expect(TokenKind::Star);
        expect(TokenKind::From);
        Select select;
        select.table = expect_ident("table name");

        if (peek().kind == TokenKind::Where) {
            advance();
            select.filter.column = expect_ident("column name");
            if (peek().kind == TokenKind::Eq) {
                advance();
                select.filter.kind = FilterKind::Eq;
                select.filter.left = parse_value();
            } else if (peek().kind == TokenKind::Between) {
                advance();
                select.filter.kind = FilterKind::Between;
                select.filter.left = parse_value();
                expect(TokenKind::And);
                select.filter.right = parse_value();
            } else {
                fail("expected WHERE comparison");
            }
        }

        return select;
    }

    Value parse_value() {
        switch (peek().kind) {
        case TokenKind::IntLit:
            return Value::Int(advance().int_value);
        case TokenKind::StrLit:
            return Value::Varchar(advance().text);
        case TokenKind::True:
            advance();
            return Value::Bool(true);
        case TokenKind::False:
            advance();
            return Value::Bool(false);
        default:
            fail("expected SQL value");
        }
    }

    std::vector<Token> tokens_;
    std::size_t pos_ = 0;
};

Statement parse(std::vector<Token> tokens) {
    return Parser(std::move(tokens)).parse();
}

} // namespace sql

} // namespace gatidb
