#include "util/Timer.hpp"


TimingProcess::TimingProcess(Timer &timer, std::string name)
    : timer(timer)
{
    timer.start(name);
}

TimingProcess::~TimingProcess()
{
    timer.stop();
}
