#include "common.hxx"
#include "signalhandler.hxx"

using namespace credativ;

JobSignalHandler::JobSignalHandler() {}

JobSignalHandler::~JobSignalHandler() {}

ConditionalSignalHandler::ConditionalSignalHandler()
  : JobSignalHandler() {

  this->ref_bvalue = NULL;

}

ConditionalSignalHandler::ConditionalSignalHandler(volatile bool *bvalue)
  : JobSignalHandler() {

  this->ref(bvalue);

}

ConditionalSignalHandler::~ConditionalSignalHandler() {}

bool ConditionalSignalHandler::ref(volatile bool *bvalue) {

  if (bvalue != NULL) {
    this->ref_bvalue = bvalue;
    return true;
  } else
    return false;
}

bool ConditionalSignalHandler::check() {

  return *(this->ref_bvalue);

}
