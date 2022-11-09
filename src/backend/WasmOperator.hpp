#pragma once

#include "backend/PhysicalOperator.hpp"
#include "backend/WasmUtil.hpp"
#include <mutable/storage/DataLayoutFactory.hpp>


namespace m {

#define M_WASM_OPERATOR_LIST(X) \
    X(WasmNoOp) \
    X(WasmCallback) \
    X(WasmPrint) \
    X(WasmScan) \
    X(WasmFilter) \
    X(WasmProjection) \
    X(WasmGrouping) \
    X(WasmAggregation) \
    X(WasmSorting) \
    X(WasmNestedLoopsJoin) \
    X(WasmLimit)

#define DECLARE(OP) \
    namespace wasm { struct OP; } \
    template<> struct Match<wasm::OP>;
    M_WASM_OPERATOR_LIST(DECLARE)
#undef DECLARE

namespace wasm {

struct WasmNoOp : PhysicalOperator<WasmNoOp, NoOpOperator>
{
    static void execute(const Match<WasmNoOp> &M, callback_t Pipeline);
    static double cost(const Match<WasmNoOp>&) { return 1.0; }
};

struct WasmCallback : PhysicalOperator<WasmCallback, CallbackOperator>
{
    static void execute(const Match<WasmCallback> &M, callback_t Pipeline);
    static double cost(const Match<WasmCallback>&) { return 1.0; }
};

struct WasmPrint : PhysicalOperator<WasmPrint, PrintOperator>
{
    static void execute(const Match<WasmPrint> &M, callback_t Pipeline);
    static double cost(const Match<WasmPrint>&) { return 1.0; }
};

struct WasmScan : PhysicalOperator<WasmScan, ScanOperator>
{
    static void execute(const Match<WasmScan> &M, callback_t Pipeline);
    static double cost(const Match<WasmScan>&) { return 1.0; }
};

struct WasmFilter : PhysicalOperator<WasmFilter, FilterOperator>
{
    static void execute(const Match<WasmFilter> &M, callback_t Pipeline);
    static double cost(const Match<WasmFilter>&) { return 1.0; }
};

struct WasmProjection : PhysicalOperator<WasmProjection, ProjectionOperator>
{
    static void execute(const Match<WasmProjection> &M, callback_t Pipeline);
    static double cost(const Match<WasmProjection>&) { return 1.0; }
    static Condition adapt_post_condition(const Match<WasmProjection> &M, const Condition &post_cond_child);
};

struct WasmGrouping : PhysicalOperator<WasmGrouping, GroupingOperator>
{
    static void execute(const Match<WasmGrouping>&, callback_t) { M_unreachable("not implemented"); }
    static double cost(const Match<WasmGrouping>&) { return 1.0; }
};

struct WasmAggregation : PhysicalOperator<WasmAggregation, AggregationOperator>
{
    static void execute(const Match<WasmAggregation>&, callback_t) { M_unreachable("not implemented"); }
    static double cost(const Match<WasmAggregation>&) { return 1.0; }
};

struct WasmSorting : PhysicalOperator<WasmSorting, SortingOperator>
{
    static void execute(const Match<WasmSorting> &M, callback_t Pipeline);
    static double cost(const Match<WasmSorting>&) { return 1.0; }
    static Condition adapt_post_condition(const Match<WasmSorting> &M, const Condition &post_cond_child);
};

struct WasmNestedLoopsJoin : PhysicalOperator<WasmNestedLoopsJoin, JoinOperator>
{
    static void execute(const Match<WasmNestedLoopsJoin> &M, callback_t Pipeline);
    static double cost(const Match<WasmNestedLoopsJoin>&) { return 1.0; }
    static Condition post_condition(const Match<WasmNestedLoopsJoin> &M);
};

struct WasmLimit : PhysicalOperator<WasmLimit, LimitOperator>
{
    static void execute(const Match<WasmLimit> &M, callback_t Pipeline);
    static double cost(const Match<WasmLimit>&) { return 1.0; }
};

}

template<>
struct Match<wasm::WasmNoOp> : MatchBase
{
    const MatchBase &child;

    Match(const NoOpOperator*, std::vector<std::reference_wrapper<const MatchBase>> &&children)
        : child(children[0])
    {
        M_insist(children.size() == 1);
    }

    void execute(callback_t Pipeline) const override { wasm::WasmNoOp::execute(*this, std::move(Pipeline)); }
    const char * name() const override { return "WasmNoOp"; }
};

template<>
struct Match<wasm::WasmCallback> : MatchBase
{
    const CallbackOperator &callback;
    const MatchBase &child;
    std::unique_ptr<const storage::DataLayoutFactory> result_set_factory;
    std::optional<std::size_t> buffer_num_tuples_;

    Match(const CallbackOperator *Callback, std::vector<std::reference_wrapper<const MatchBase>> &&children)
        : callback(*Callback)
        , child(children[0])
        , result_set_factory(std::make_unique<storage::RowLayoutFactory>()) // TODO: let optimizer decide this
    {
        M_insist(children.size() == 1);
    }

    void execute(callback_t Pipeline) const override { wasm::WasmCallback::execute(*this, std::move(Pipeline)); }
    const char * name() const override { return "WasmCallback"; }
};

template<>
struct Match<wasm::WasmPrint> : MatchBase
{
    const PrintOperator &print;
    const MatchBase &child;
    std::unique_ptr<const storage::DataLayoutFactory> result_set_factory;
    std::optional<std::size_t> buffer_num_tuples_;

    Match(const PrintOperator *print, std::vector<std::reference_wrapper<const MatchBase>> &&children)
        : print(*print)
        , child(children[0])
        , result_set_factory(std::make_unique<storage::RowLayoutFactory>()) // TODO: let optimizer decide this
    {
        M_insist(children.size() == 1);
    }

    void execute(callback_t Pipeline) const override { wasm::WasmPrint::execute(*this, std::move(Pipeline)); }
    const char * name() const override { return "WasmPrint"; }
};

template<>
struct Match<wasm::WasmScan> : MatchBase
{
    private:
    std::unique_ptr<const storage::DataLayoutFactory> buffer_factory_;
    std::optional<std::size_t> buffer_num_tuples_;
    public:
    const ScanOperator &scan;

    Match(const ScanOperator *scan, std::vector<std::reference_wrapper<const MatchBase>> &&children)
        : scan(*scan)
    {
        M_insist(children.empty());
    }

    void execute(callback_t Pipeline) const override {
        if (buffer_factory_) {
            M_insist(bool(buffer_num_tuples_));
            M_insist(scan.schema() == scan.schema().drop_none().deduplicate(),
                     "schema of `ScanOperator` must not contain NULL or duplicates");
            M_insist(scan.schema().num_entries(), "schema of `ScanOperator` must not be empty");
            wasm::LocalBuffer buffer(scan.schema(), *buffer_factory_, *buffer_num_tuples_, std::move(Pipeline));
            wasm::WasmScan::execute(*this, std::bind(&wasm::LocalBuffer::consume, &buffer));
            buffer.resume_pipeline();
        } else {
            wasm::WasmScan::execute(*this, std::move(Pipeline));
        }
    }

    const char * name() const override { return "WasmScan"; }
};

template<>
struct Match<wasm::WasmFilter> : MatchBase
{
    private:
    std::unique_ptr<const storage::DataLayoutFactory> buffer_factory_;
    std::optional<std::size_t> buffer_num_tuples_;
    public:
    const FilterOperator &filter;
    const MatchBase &child;

    Match(const FilterOperator *filter, std::vector<std::reference_wrapper<const MatchBase>> &&children)
        : filter(*filter)
        , child(children[0])
    {
        M_insist(children.size() == 1);
    }

    void execute(callback_t Pipeline) const override {
        if (buffer_factory_) {
            M_insist(bool(buffer_num_tuples_));
            auto buffer_schema = filter.schema().drop_none().deduplicate();
            if (buffer_schema.num_entries()) {
                wasm::LocalBuffer buffer(buffer_schema, *buffer_factory_, *buffer_num_tuples_, std::move(Pipeline));
                wasm::WasmFilter::execute(*this, std::bind(&wasm::LocalBuffer::consume, &buffer));
                buffer.resume_pipeline();
            } else {
                wasm::WasmFilter::execute(*this, std::move(Pipeline));
            }
        } else {
            wasm::WasmFilter::execute(*this, std::move(Pipeline));
        }
    }

    const char * name() const override { return "WasmFilter"; }
};

template<>
struct Match<wasm::WasmProjection> : MatchBase
{
    const ProjectionOperator &projection;
    std::optional<std::reference_wrapper<const MatchBase>> child;

    Match(const ProjectionOperator *projection, std::vector<std::reference_wrapper<const MatchBase>> &&children)
        : projection(*projection)
    {
        if (not children.empty()) {
            M_insist(children.size() == 1);
            child = std::move(children[0]);
        }
    }

    void execute(callback_t Pipeline) const override { wasm::WasmProjection::execute(*this, std::move(Pipeline)); }
    const char * name() const override { return "WasmProjection"; }
};

template<>
struct Match<wasm::WasmGrouping> : MatchBase
{
    const GroupingOperator &grouping;
    const MatchBase &child;

    Match(const GroupingOperator *grouping, std::vector<std::reference_wrapper<const MatchBase>> &&children)
        : grouping(*grouping)
        , child(children[0])
    {
        M_insist(children.size() == 1);
    }

    void execute(callback_t Pipeline) const override { wasm::WasmGrouping::execute(*this, std::move(Pipeline)); }
    const char * name() const override { return "WasmGrouping"; }
};

template<>
struct Match<wasm::WasmAggregation> : MatchBase
{
    const AggregationOperator &aggregation;
    const MatchBase &child;

    Match(const AggregationOperator *aggregation, std::vector<std::reference_wrapper<const MatchBase>> &&children)
        : aggregation(*aggregation)
        , child(children[0])
    {
        M_insist(children.size() == 1);
    }

    void execute(callback_t Pipeline) const override { wasm::WasmAggregation::execute(*this, std::move(Pipeline)); }
    const char * name() const override { return "WasmAggregation"; }
};

template<>
struct Match<wasm::WasmSorting> : MatchBase
{
    const SortingOperator &sorting;
    const MatchBase &child;
    std::unique_ptr<const storage::DataLayoutFactory> materializing_factory;

    Match(const SortingOperator *sorting, std::vector<std::reference_wrapper<const MatchBase>> &&children)
        : sorting(*sorting)
        , child(children[0])
        , materializing_factory(std::make_unique<storage::RowLayoutFactory>()) // TODO: let optimizer decide this
    {
        M_insist(children.size() == 1);
    }

    void execute(callback_t Pipeline) const override { wasm::WasmSorting::execute(*this, std::move(Pipeline)); }
    const char * name() const override { return "WasmSorting"; }
};

template<>
struct Match<wasm::WasmNestedLoopsJoin> : MatchBase
{
    private:
    std::unique_ptr<const storage::DataLayoutFactory> buffer_factory_;
    std::optional<std::size_t> buffer_num_tuples_;
    public:
    const JoinOperator &join;
    std::vector<std::reference_wrapper<const MatchBase>> children;
    std::vector<std::unique_ptr<const storage::DataLayoutFactory>> materializing_factories_;

    Match(const JoinOperator *join, std::vector<std::reference_wrapper<const MatchBase>> &&children)
        : join(*join)
        , children(std::move(children))
    {
        M_insist(this->children.size() >= 2);

        for (std::size_t i = 0; i < this->children.size(); ++i)
            materializing_factories_.emplace_back(std::make_unique<storage::RowLayoutFactory>()); // TODO: let optimizer decide this
    }

    void execute(callback_t Pipeline) const override {
        if (buffer_factory_) {
            M_insist(bool(buffer_num_tuples_));
            auto buffer_schema = join.schema().drop_none().deduplicate();
            if (buffer_schema.num_entries()) {
                wasm::LocalBuffer buffer(buffer_schema, *buffer_factory_, *buffer_num_tuples_, std::move(Pipeline));
                wasm::WasmNestedLoopsJoin::execute(*this, std::bind(&wasm::LocalBuffer::consume, &buffer));
                buffer.resume_pipeline();
            } else {
                wasm::WasmNestedLoopsJoin::execute(*this, std::move(Pipeline));
            }
        } else {
            wasm::WasmNestedLoopsJoin::execute(*this, std::move(Pipeline));
        }
    }

    const char * name() const override { return "WasmNestedLoopsJoin"; }
};

template<>
struct Match<wasm::WasmLimit> : MatchBase
{
    const LimitOperator &limit;
    const MatchBase &child;

    Match(const LimitOperator *limit, std::vector<std::reference_wrapper<const MatchBase>> &&children)
        : limit(*limit)
        , child(children[0])
    {
        M_insist(children.size() == 1);
    }

    void execute(callback_t Pipeline) const override { wasm::WasmLimit::execute(*this, std::move(Pipeline)); }
    const char * name() const override { return "WasmLimit"; }
};

}
