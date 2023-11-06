#pragma once


namespace m {

namespace idx {

/** An enum class that lists all supported index methods. */
enum class IndexMethod { Array };

/** The base class for indexes. */
struct IndexBase {
    virtual ~IndexBase() { }
};

}

}

