declare -A PLANNER_CONFIGS=(
    ###### Traditional Planners #####
    [DPsub]="--plan-enumerator DPsubOpt"
    [DPccp]="--plan-enumerator DPccp"
    [TDMinCutAGaT]="--plan-enumerator TDMinCutAGaT"
    [IKKBZ]="--plan-enumerator IKKBZ"
    [linDP]="--plan-enumerator LinearizedDP"
    [GOO]="--plan-enumerator GOO"
    ##### Heuristic Search #####
    [BU-cleanAStar-zero]="--plan-enumerator HeuristicSearch --hs-vertex SubproblemsArray --hs-expand BottomUpComplete --hs-heuristic zero --hs-search cleanAStar"
    [TD-cleanAStar-zero]="--plan-enumerator HeuristicSearch --hs-vertex SubproblemsArray --hs-expand TopDownComplete --hs-heuristic zero --hs-search cleanAStar"
    ##### Hanwen Beam Search , Beamwidth = 1 #####
    [BU-beam-hanwen-zero]="--plan-enumerator HeuristicSearch --hs-vertex SubproblemsArray --hs-expand BottomUpComplete --hs-heuristic zero --hs-search beam_search_hanwen"
    [TD-beam-hanwen-zero]="--plan-enumerator HeuristicSearch --hs-vertex SubproblemsArray --hs-expand TopDownComplete --hs-heuristic zero --hs-search beam_search_hanwen"

    ##### Hanwen Layered Search #####
    [BU-hanwen-layered-zero]="--plan-enumerator HeuristicSearch --hs-vertex SubproblemsArray --hs-expand BottomUpComplete --hs-heuristic zero --hs-search hanwen_layered_search"
    [TD-hanwen-layered-zero]="--plan-enumerator HeuristicSearch --hs-vertex SubproblemsArray --hs-expand TopDownComplete --hs-heuristic zero --hs-search hanwen_layered_search"
    [BU-hanwen-layered2-zero]="--plan-enumerator HeuristicSearch --hs-vertex SubproblemsArray --hs-expand BottomUpComplete --hs-heuristic zero --hs-search hanwen_layered_search_2"
    [TD-hanwen-layered2-zero]="--plan-enumerator HeuristicSearch --hs-vertex SubproblemsArray --hs-expand TopDownComplete --hs-heuristic zero --hs-search hanwen_layered_search_2"
    [BU-hanwen-layered3-zero]="--plan-enumerator HeuristicSearch --hs-vertex SubproblemsArray --hs-expand BottomUpComplete --hs-heuristic zero --hs-search hanwen_layered_search_3"
    [TD-hanwen-layered3-zero]="--plan-enumerator HeuristicSearch --hs-vertex SubproblemsArray --hs-expand TopDownComplete --hs-heuristic zero --hs-search hanwen_layered_search_3"

    # BottomUp
    [BU-BIDIRECTIONAL-zero]="--plan-enumerator HeuristicSearch --hs-vertex SubproblemsArray --hs-expand BottomUpComplete --hs-heuristic zero --hs-search BIDIRECTIONAL"
    [BU-LAYEREDBIDIRECTIONAL-zero]="--plan-enumerator HeuristicSearch --hs-vertex SubproblemsArray --hs-expand BottomUpComplete --hs-heuristic zero --hs-search LAYEREDBIDIRECTIONAL"


    ## IDDFS
    [BU-IDDFS-zero]="--plan-enumerator HeuristicSearch --hs-vertex SubproblemsArray --hs-expand BottomUpComplete --hs-heuristic zero --hs-search IDDFS"

    ## A*
    [BU-A*-zero]="--plan-enumerator HeuristicSearch --hs-vertex SubproblemsArray --hs-expand BottomUpComplete --hs-heuristic zero --hs-search AStar"
    [BU-A*-avg_sel]="--plan-enumerator HeuristicSearch --hs-vertex SubproblemsArray --hs-expand BottomUpComplete --hs-heuristic avg_sel --hs-search AStar"
    [BU-A*-GOO]="--plan-enumerator HeuristicSearch --hs-vertex SubproblemsArray --hs-expand BottomUpComplete --hs-heuristic GOO --hs-search AStar"
    [BU-A*-sum]="--plan-enumerator HeuristicSearch --hs-vertex SubproblemsArray --hs-expand BottomUpComplete  --hs-heuristic sum --hs-search AStar"
    [BU-A*-sqrt_sum]="--plan-enumerator HeuristicSearch --hs-vertex SubproblemsArray --hs-expand BottomUpComplete --hs-heuristic sqrt_sum --hs-search AStar"
    [BU-A*-scaled_sum]="--plan-enumerator HeuristicSearch --hs-vertex SubproblemsArray --hs-expand BottomUpComplete --hs-heuristic scaled_sum --hs-search AStar"
    ## beam
    [BU-beam-zero]="--plan-enumerator HeuristicSearch --hs-vertex SubproblemsArray --hs-expand BottomUpComplete --hs-heuristic zero --hs-search monotone_beam_search"
    [BU-beam-avg_sel]="--plan-enumerator HeuristicSearch --hs-vertex SubproblemsArray --hs-expand BottomUpComplete --hs-heuristic avg_sel --hs-search monotone_beam_search"
    [BU-beam-GOO]="--plan-enumerator HeuristicSearch --hs-vertex SubproblemsArray --hs-expand BottomUpComplete --hs-heuristic GOO --hs-search monotone_beam_search"
    ## relative beam
    [BU-rel_beam-zero]="--plan-enumerator HeuristicSearch --hs-vertex SubproblemsArray --hs-expand BottomUpComplete --hs-heuristic zero --hs-search monotone_dynamic_beam_search"
    # TopDown
    ## A*
    [TD-A*-zero]="--plan-enumerator HeuristicSearch --hs-vertex SubproblemsArray --hs-expand TopDownComplete  --hs-heuristic zero --hs-search AStar"
    [TD-A*-sum]="--plan-enumerator HeuristicSearch --hs-vertex SubproblemsArray --hs-expand TopDownComplete  --hs-heuristic sum --hs-search AStar"
    [TD-A*-GOO]="--plan-enumerator HeuristicSearch --hs-vertex SubproblemsArray --hs-expand TopDownComplete  --hs-heuristic GOO --hs-search AStar"
    [TD-A*-sqrt_sum]="--plan-enumerator HeuristicSearch --hs-vertex SubproblemsArray --hs-expand TopDownComplete --hs-heuristic sqrt_sum --hs-search AStar"
    [TD-A*-scaled_sum]="--plan-enumerator HeuristicSearch --hs-vertex SubproblemsArray --hs-expand TopDownComplete --hs-heuristic scaled_sum --hs-search AStar"
    ## beam
    [TD-beam-zero]="--plan-enumerator HeuristicSearch --hs-vertex SubproblemsArray --hs-expand TopDownComplete  --hs-heuristic zero --hs-search monotone_beam_search"
    [TD-beam-sum]="--plan-enumerator HeuristicSearch --hs-vertex SubproblemsArray --hs-expand TopDownComplete  --hs-heuristic sum --hs-search monotone_beam_search"
    [TD-beam-GOO]="--plan-enumerator HeuristicSearch --hs-vertex SubproblemsArray --hs-expand TopDownComplete  --hs-heuristic GOO --hs-search monotone_beam_search"
    ## relative beam
    [TD-rel_beam-zero]="--plan-enumerator HeuristicSearch --hs-vertex SubproblemsArray --hs-expand TopDownComplete  --hs-heuristic zero --hs-search monotone_dynamic_beam_search"
)
