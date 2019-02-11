#pragma once

#include <streambuf>


class NullBuffer : public std::streambuf
{
    public:
    int overflow(int c) { return c; }
};

class NullStream : public std::ostream
{
    public:
    NullStream() : std::ostream(&m_sb) {}
    private:
    NullBuffer m_sb;
};
