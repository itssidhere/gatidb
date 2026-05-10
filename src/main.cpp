#include "gatidb/gatidb.hpp"

#include <filesystem>
#include <iostream>

int main() {
    using namespace gatidb;

    const std::string db_file = "gatidb.db";
    const std::string wal_file = "gatidb.wal";

    {
        DiskManager disk(db_file);
        Wal wal(wal_file);
        BufferPool pool(std::move(disk), std::move(wal), 64);
        Catalog catalog(std::move(pool));

        catalog.create_table(
            "jobs",
            Schema{
                {
                    Column{"id", DataType::Int()},
                    Column{"title", DataType::Varchar(64)},
                    Column{"done", DataType::Bool()},
                },
                0,
            },
            3
        );

        auto table = catalog.get_table("jobs").value();
        table.insert_row({Value::Int(1), Value::Varchar("fix bug"), Value::Bool(false)});
        catalog.update_table_storage(table);
        catalog.flush();
        std::cout << "Written\n";
    }

    {
        DiskManager disk(db_file);
        Wal wal(wal_file);
        BufferPool pool(std::move(disk), std::move(wal), 64);
        Catalog catalog(std::move(pool));

        auto table = catalog.get_table("jobs").value();
        auto row = table.get_row(1);
        if (row) {
            std::cout << "Read back:";
            for (const auto& value : *row) {
                std::cout << ' ' << value.to_string();
            }
            std::cout << '\n';
        }
    }

    std::filesystem::remove(db_file);
    std::filesystem::remove(wal_file);
    return 0;
}
