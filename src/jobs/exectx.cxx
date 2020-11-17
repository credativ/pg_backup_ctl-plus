#include <memory>
#include <exectx.hxx>
#include <pgbckctl_exception.hxx>

using namespace credativ;
using namespace credativ::pgprotocol;

/* ****************************************************************************
 * ExecutableContext
 * ****************************************************************************/

ExecutableContext::ExecutableContext() {}

ExecutableContext::~ExecutableContext() {}

ExecutableContextName ExecutableContext::getName() {
  return name;
}

std::shared_ptr<ExecutableContext> ExecutableContext::create(ExecutableContextName name) {

  std::shared_ptr<ExecutableContext> context = nullptr;

  switch(name) {

  case EXECUTABLE_CONTEXT_DEFAULT:
    {
      context = std::make_shared<ExecutableContext>();
      break;
    }

  case EXECUTABLE_CONTEXT_COPY_BOTH:
    {
      context = std::make_shared<CopyBothExecutableContext>();
      break;
    }

  case EXECUTABLE_CONTEXT_ERROR:
    throw CPGBackupCtlFailure("executable context cannot be created");
    break;

  }

  return context;

}

/* ****************************************************************************
 * CopyBothExecutableContext
 * ****************************************************************************/

CopyBothExecutableContext::CopyBothExecutableContext() {}

CopyBothExecutableContext::~CopyBothExecutableContext() {}
