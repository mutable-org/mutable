#pragma once


namespace db {

/** This is a stub for the cost model class. */
struct CostModel
{
    virtual ~CostModel() { }
};

struct DummyCostModel : CostModel
{
};

}
