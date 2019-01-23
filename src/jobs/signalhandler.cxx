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

  bool flag = false;

  /*
   * Having a NULL pointer here causes
   * chech() to always return FALSE.
   */
  if (this->ref_bvalue == nullptr)
    return false;

  /*
   * Reset flag.
   */
  flag = *(this->ref_bvalue);
  *(this->ref_bvalue) = false;

  return flag;

}

AtomicSignalHandler::AtomicSignalHandler() {

  this->ref_var = nullptr;
  this->ref_value = -1;

}

AtomicSignalHandler::AtomicSignalHandler(volatile sig_atomic_t *ref_var,
                                         int ref_value) {

  this->ref_var = ref_var;
  this->ref_value = ref_value;

}

AtomicSignalHandler::~AtomicSignalHandler() {}

int AtomicSignalHandler::ref(volatile sig_atomic_t *ref_var,
                             int ref_value) {

  if (ref_var != nullptr) {
    this->ref_var = ref_var;
    this->ref_value = ref_value;
  }

  return this->ref_value;

}

bool AtomicSignalHandler::check() {

  if (this->ref_var != nullptr) {
    return (*(this->ref_var) == this->ref_value);
  }

  return false;
}
