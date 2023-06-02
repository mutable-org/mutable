declare -A PLANNER_CONFIGS=(
    ###### Traditional Planners #####
    [PEall]="--plan-enumerator PEall"
    [DPsub]="--plan-enumerator DPsubOpt"
    [DPccp]="--plan-enumerator DPccp"
    [TDMinCutAGaT]="--plan-enumerator TDMinCutAGaT"
    [IKKBZ]="--plan-enumerator IKKBZ"
    [linDP]="--plan-enumerator LinearizedDP"
    [GOO]="--plan-enumerator GOO"
    ##### Heuristic Search #####
    # BottomUp
    ## A*
    [BU-A*-zero]="--plan-enumerator HeuristicSearch --hs-vertex SubproblemsArray --hs-expand BottomUpComplete --hs-heuristic zero --hs-search AStar"
    [BU-A*-avg_sel]="--plan-enumerator HeuristicSearch --hs-vertex SubproblemsArray --hs-expand BottomUpComplete --hs-heuristic avg_sel --hs-search AStar"
    [BU-A*-GOO]="--plan-enumerator HeuristicSearch --hs-vertex SubproblemsArray --hs-expand BottomUpComplete --hs-heuristic GOO --hs-search AStar"
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
    ## beam
    [TD-beam-zero]="--plan-enumerator HeuristicSearch --hs-vertex SubproblemsArray --hs-expand TopDownComplete  --hs-heuristic zero --hs-search monotone_beam_search"
    [TD-beam-sum]="--plan-enumerator HeuristicSearch --hs-vertex SubproblemsArray --hs-expand TopDownComplete  --hs-heuristic sum --hs-search monotone_beam_search"
    [TD-beam-GOO]="--plan-enumerator HeuristicSearch --hs-vertex SubproblemsArray --hs-expand TopDownComplete  --hs-heuristic GOO --hs-search monotone_beam_search"
    ## relative beam
    [TD-rel_beam-zero]="--plan-enumerator HeuristicSearch --hs-vertex SubproblemsArray --hs-expand TopDownComplete  --hs-heuristic zero --hs-search monotone_dynamic_beam_search"
)
