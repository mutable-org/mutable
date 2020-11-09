#include "mutable/backend/Backend.hpp"


using namespace m;


const std::unordered_map<std::string, Backend::kind_t> Backend::STR_TO_KIND = {
#define DB_BACKEND(NAME, _) { #NAME, B_ ## NAME },
#include "mutable/tables/Backend.tbl"
#undef DB_BACKEND
};

std::unique_ptr<Backend> Backend::Create(Backend::kind_t kind) {
    switch (kind) {
#define DB_BACKEND(NAME, _) case B_ ## NAME: return Create ## NAME();
#include "mutable/tables/Backend.tbl"
#undef DB_BACKEND
    }
}
