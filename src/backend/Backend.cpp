#include "backend/Backend.hpp"


using namespace db;


const std::unordered_map<std::string, Backend::kind_t> Backend::STR_TO_KIND = {
#define DB_BACKEND(NAME, _) { #NAME, B_ ## NAME },
#include "tables/Backend.tbl"
#undef DB_BACKEND
};

std::unique_ptr<Backend> Backend::Create(Backend::kind_t kind) {
    switch (kind) {
#define DB_BACKEND(NAME, _) case B_ ## NAME: return Create ## NAME();
#include "tables/Backend.tbl"
#undef DB_BACKEND
    }
}
