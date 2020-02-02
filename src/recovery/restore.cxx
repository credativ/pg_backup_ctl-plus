#include <common.hxx>
#include <recovery.hxx>

using namespace credativ;

Recovery::Recovery() {}
Recovery::Recovery(std::shared_ptr<RestoreStreamDescr> restoreDescr) {}

Recovery::~Recovery() {}

TarRecovery::TarRecovery(std::shared_ptr<RestoreStreamDescr> restoreDescr) {}

TarRecovery::~TarRecovery() {}

void TarRecovery::init() {}
