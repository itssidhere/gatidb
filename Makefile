CXX ?= c++
CXXFLAGS ?= -std=c++20 -Wall -Wextra -Wpedantic -O2
CPPFLAGS ?= -Iinclude
BUILD_DIR ?= build

ENGINE_SRC := src/gatidb.cpp
HEADER := include/gatidb/gatidb.h

.PHONY: all test demo clean

all: $(BUILD_DIR)/gatidb $(BUILD_DIR)/gatidb_tests

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(BUILD_DIR)/gatidb: $(ENGINE_SRC) src/main.cpp $(HEADER) | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) $(ENGINE_SRC) src/main.cpp -o $@

$(BUILD_DIR)/gatidb_tests: $(ENGINE_SRC) tests/test_gatidb.cpp $(HEADER) | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) $(ENGINE_SRC) tests/test_gatidb.cpp -o $@

test: $(BUILD_DIR)/gatidb_tests
	./$(BUILD_DIR)/gatidb_tests

demo: $(BUILD_DIR)/gatidb
	./$(BUILD_DIR)/gatidb

clean:
	rm -rf $(BUILD_DIR)
