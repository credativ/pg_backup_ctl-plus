#include <rtconfig.hxx>

using namespace pgbckctl;
using namespace std;

/* *****************************************************************************
 * ConfigVariable Base class
 * ****************************************************************************/

string ConfigVariable::getName() {
  return this->name;
}

void ConfigVariable::set_assign_hook(config_variable_assign_hook ahook) {

  if (ahook != NULL) {
    assign_hook = ahook;
  }

}

void ConfigVariable::reassign() {

  if (this->assign_hook != NULL) {

    string value;

    this->getValue(value);
    this->assign_hook(value);

  }

}

void ConfigVariable::setValue(string value) {
  throw CPGBackupCtlFailure("cannot use runtime variable assignment in default implementation");
}

void ConfigVariable::setValue(int value) {
  throw CPGBackupCtlFailure("cannot use runtime variable assignment in default implementation");
}

void ConfigVariable::setValue(bool value) {
  throw CPGBackupCtlFailure("cannot use runtime variable assignment in default implementation");
}

void ConfigVariable::setDefault(string defaultval) {
  throw CPGBackupCtlFailure("cannot use runtime variable assignment in default implementation");
}

void ConfigVariable::setDefault(int defaultval) {
  throw CPGBackupCtlFailure("cannot use runtime variable assignment in default implementation");
}

void ConfigVariable::setDefault(bool defaultval) {
  throw CPGBackupCtlFailure("cannot use runtime variable assignment in default implementation");
}

void ConfigVariable::setRange(int min, int max) {
  throw CPGBackupCtlFailure("enforcing range not possible in base configuration implementation");
}

bool ConfigVariable::enforceRangeConstraint(bool force) {
  throw CPGBackupCtlFailure("setting range checks not possible in base configuration implementation");
}

void ConfigVariable::getValue(std::string &value) {
  throw CPGBackupCtlFailure("cannot retrieve value from base configuration class");
}

void ConfigVariable::getValue(bool &value) {
  throw CPGBackupCtlFailure("cannot retrieve value from base configuration class");
}

void ConfigVariable::getValue(int &value) {
  throw CPGBackupCtlFailure("cannot retrieve value from base configuration class");
}

void ConfigVariable::getDefault(std::string &value) {
  throw CPGBackupCtlFailure("cannot retrieve default value from base configuration class");
}

void ConfigVariable::getDefault(bool &value) {
  throw CPGBackupCtlFailure("cannot retrieve default value from base configuration class");
}

void ConfigVariable::getDefault(int &value) {
  throw CPGBackupCtlFailure("cannot retrieve default value from base configuration class");
}

void ConfigVariable::reset() {
  throw CPGBackupCtlFailure("cannot reset default value within base configuration class");
}

/* *****************************************************************************
 * BoolConfigVariable Runtime Variable Implementation
 * ****************************************************************************/

BoolConfigVariable::BoolConfigVariable(string name) {

  this->name = name;

}

void BoolConfigVariable::reset() {

  this->value = this->default_value;

}

BoolConfigVariable::BoolConfigVariable(string name,
                                       bool   value,
                                       bool   defaultval) : BoolConfigVariable(name) {

  this->value = value;
  this->default_value = defaultval;

}

void BoolConfigVariable::setValue(bool value) {

  this->value = value;

  if (this->assign_hook != NULL) {
    string valstr;

    this->getValue(valstr);
    this->assign_hook(valstr);
  }

}

void BoolConfigVariable::setDefault(bool value) {

  this->default_value = value;

}

void BoolConfigVariable::getValue(bool &value) {
  value = this->value;
}

void BoolConfigVariable::getValue(string &value) {

  (this->value) ? value = "true" : value = "false";

}

void BoolConfigVariable::getDefault(bool &value) {
  value = this->default_value;
}

/* *****************************************************************************
 * StringConfigVariable Runtime Variable Implementation
 * ****************************************************************************/

StringConfigVariable::StringConfigVariable(string name,
                                           string value,
                                           string defaultval) : StringConfigVariable(name) {

  this->name = name;
  this->value = value;
  this->default_value = defaultval;

}

void StringConfigVariable::reset() {

  this->value = this->default_value;

}

StringConfigVariable::StringConfigVariable(string name) : StringConfigVariable() {

  this->name = name;

}

StringConfigVariable::StringConfigVariable() {}

void StringConfigVariable::setValue(string value) {

  this->value = value;

  if (this->assign_hook != NULL) {

    string valstr;

    this->getValue(valstr);
    this->assign_hook(valstr);

  }

}

void StringConfigVariable::setDefault(string defaultval) {

  this->default_value = defaultval;

}

void StringConfigVariable::getValue(string &value) {
  value = this->value;
}

void StringConfigVariable::getDefault(string &value) {
  value = this->default_value;
}

/* *****************************************************************************
 * EnumConfigVariable Runtime Variable Implementation
 * ****************************************************************************/

EnumConfigVariable::EnumConfigVariable(string name,
                                       string value,
                                       string defaultval,
                                       std::unordered_set<string> possible_values) : EnumConfigVariable(name) {

  /* IMPORTANT: set allowed values *before* assigning the value */
  this->allowed_values = possible_values;

  this->setValue(value);
  this->setDefault(defaultval);

}

void EnumConfigVariable::reset() {

  this->value = this->default_value;

}

EnumConfigVariable::EnumConfigVariable(string name) {

  this->name = name;

}

EnumConfigVariable::EnumConfigVariable(string name,
                                       unordered_set<string> possible_values) : EnumConfigVariable(name) {

  this->allowed_values = possible_values;

}

void EnumConfigVariable::getDefault(string &value) {
  value = this->default_value;
}

void EnumConfigVariable::getValue(string &value) {
  value = this->value;
}

void EnumConfigVariable::addAllowedValue(string allowed_value) {

  this->allowed_values.insert(allowed_value);

}

void EnumConfigVariable::check_value(std::string value) {

  unordered_set<string>::const_iterator gotit = this->allowed_values.find(value);

  if (gotit == this->allowed_values.end()) {

    ostringstream oss;

    oss << "invalid value \""
        << value
        << "\" for variable "
        << this->name;

    /* this value is not in the list of allowed values */
    throw CPGBackupCtlFailure(oss.str());

  }

}

void EnumConfigVariable::setValue(string value) {

  /*
   * Before assigning the new value, check if
   * the string is in the list of allowed values.
   */
  this->check_value(value);
  this->value = value;

  /* call assign_hook if available */
  if (this->assign_hook != NULL) {

    string valstr;

    this->getValue(valstr);
    this->assign_hook(valstr);

  }

}

void EnumConfigVariable::setDefault(string defaultval) {

  /*
   * Before assigning the new value, check if
   * the string is in the list of allowed values.
   */
  this->check_value(defaultval);
  this->value = defaultval;

}

/* *****************************************************************************
 * IntegerConfigVariable Runtime Variable Implementation
 * ****************************************************************************/

IntegerConfigVariable::IntegerConfigVariable(string name) {

  this->name = name;

}

IntegerConfigVariable::IntegerConfigVariable(string name,
                                             int value,
                                             int default_value,
                                             int range_min,
                                             int range_max,
                                             bool enforceRangeConstraint) : IntegerConfigVariable(name) {

  this->setRange(range_min, range_max);
  this->enforceRangeConstraint(enforceRangeConstraint);

  this->setDefault(default_value);
  this->setValue(value);

}

IntegerConfigVariable::IntegerConfigVariable(string name,
                                             int value,
                                             int defaultval,
                                             bool enforceRangeConstraint) : IntegerConfigVariable(name) {

  /* IMPORTANT: set range constraint property before assigning values */
  this->enforceRangeConstraint(enforceRangeConstraint);

  this->setValue(value);
  this->setDefault(defaultval);

}

void IntegerConfigVariable::getDefault(int &value) {
  value = this->default_value;
}

void IntegerConfigVariable::getValue(string &value) {

  value = CPGBackupCtlBase::intToStr(this->value);

}

void IntegerConfigVariable::getValue(int &value) {
  value = this->value;
}

void IntegerConfigVariable::reset() {

  this->value = this->default_value;

}

void IntegerConfigVariable::check(int value) {


  if (this->enforce_rangecheck
      && (value > this->max || value < this->min)) {

    ostringstream oss;
    oss << "value "
        << value
        << " violates allowed range of values: min="
        << this->min
        << " max="
        << this->max;
    throw CPGBackupCtlFailure(oss.str());

  }

}

void IntegerConfigVariable::setRange(int min, int max) {

  if (max < min)
    throw CPGBackupCtlFailure("max value smaller than min when setting configuration value range");

  this->min = min;
  this->max = max;

}

bool IntegerConfigVariable::enforceRangeConstraint(bool force) {

  bool oldval = this->enforce_rangecheck;

  this->enforce_rangecheck = force;

  /* if turned on, force a check against current values */
  if (oldval || this->enforce_rangecheck) {
    this->check(this->value);
    this->check(this->default_value);
  }

  return oldval;

}

void IntegerConfigVariable::setValue(int value) {

  this->check(value);
  this->value = value;

    /* call assign_hook if available */
  if (this->assign_hook != NULL) {

    string valstr;

    this->getValue(valstr);
    this->assign_hook(valstr);

  }

}

void IntegerConfigVariable::setDefault(int defaultval) {

  this->check(defaultval);
  this->default_value = defaultval;

}

/* *****************************************************************************
 * Runtime environment implementation
 * ****************************************************************************/

RuntimeVariableEnvironment::RuntimeVariableEnvironment(shared_ptr<RuntimeConfiguration> rtc) {
  this->runtime_config = rtc;
}

shared_ptr<RuntimeConfiguration> RuntimeVariableEnvironment::createRuntimeConfiguration() {
  return make_shared<RuntimeConfiguration>();
}

shared_ptr<RuntimeConfiguration> RuntimeVariableEnvironment::getRuntimeConfiguration() {
  return this->runtime_config;
}

void RuntimeVariableEnvironment::assignRuntimeConfiguration(std::shared_ptr<RuntimeConfiguration> ptr) {
  this->runtime_config = ptr;
}

/* *****************************************************************************
 * Runtime Configuration Implementation
 * ****************************************************************************/

RuntimeConfiguration::RuntimeConfiguration() {}

RuntimeConfiguration::~RuntimeConfiguration() {}

size_t RuntimeConfiguration::count_variables() {
  return variables.size();
}

config_variable_iterator RuntimeConfiguration::end() {
  return this->variables.end();
}

config_variable_iterator RuntimeConfiguration::begin() {
  return this->variables.begin();
}

void RuntimeConfiguration::reset(string name) {

  shared_ptr<ConfigVariable> var = nullptr;

  auto it = this->variables.find(name);

  if (it != this->variables.end()) {

    var = it->second;

  } else {

    throw CPGBackupCtlFailure("variable does not exist: \"" + name + "\"");

  }

}

shared_ptr<ConfigVariable> RuntimeConfiguration::get(string name) {

  shared_ptr<ConfigVariable> var = nullptr;

  auto it = this->variables.find(name);

  if (it != this->variables.end()) {

    var = it->second;

  } else {

    throw CPGBackupCtlFailure("variable does not exist: \"" + name + "\"");

  }

  return var;

}

std::shared_ptr<ConfigVariable> RuntimeConfiguration::create(string name,
                                                             bool value,
                                                             bool default_value) {
  shared_ptr<ConfigVariable> var = nullptr;

  /*
   * Check if the variable already exists. If true, assign
   * the new value and default_value.
   */
  auto it = this->variables.find(name);

  if (it != this->variables.end()) {

    var = it->second;
    var->setDefault(default_value);
    var->setValue(value);

  } else {

    var = make_shared<BoolConfigVariable>(name, value, default_value);
    this->variables.insert(make_pair(name, var));

  }

  return var;

}

std::shared_ptr<ConfigVariable> RuntimeConfiguration::create(string name, int value, int default_value,
                                                             int range_min, int range_max) {

  shared_ptr<ConfigVariable> var = nullptr;

  /*
   * Check if the variable already exists. If true, assign
   * the new value and default_value.
   */
  auto it = this->variables.find(name);

  if (it != this->variables.end()) {

    var = it->second;
    var->setDefault(default_value);
    var->setRange(range_min, range_max);
    var->enforceRangeConstraint(true);
    var->setValue(value);

  } else {

    var = make_shared<IntegerConfigVariable>(name, value, default_value, range_min, range_max);
    this->variables.insert(make_pair(name, var));

  }

  return var;

}

std::shared_ptr<ConfigVariable> RuntimeConfiguration::create(string name,
                                                             string value,
                                                             string default_value,
                                                             std::unordered_set<string> possible_values) {

  shared_ptr<ConfigVariable> var = nullptr;

  /*
   * Check if the variable already exists. If true, assign
   * the new value and default_value.
   */
  auto it = this->variables.find(name);

  if (it != this->variables.end()) {

    var = it->second;
    var->setValue(value);

  } else {

    var = make_shared<EnumConfigVariable>(name,
                                          value,
                                          default_value,
                                          possible_values);
    this->variables.insert(make_pair(name, var));

  }

  return var;
}

std::shared_ptr<ConfigVariable> RuntimeConfiguration::create(string name, int value, int default_value) {

  shared_ptr<ConfigVariable> var = nullptr;

  /*
   * Check if the variable already exists. If true, just
   * assign the new value.
   */
  auto it = this->variables.find(name);

  if (it != this->variables.end()) {

    var = it->second;
    var->setValue(value);

  } else {

    /*
     * Make the configuration variable first and then
     * assign new values. If we fail here, the pointer will be
     * released automatically, since it won't get into
     * the map.
     */
    var = make_shared<IntegerConfigVariable>(name, value, default_value);
    this->variables.insert(make_pair(name, var));

  }

  return var;

}

std::shared_ptr<ConfigVariable> RuntimeConfiguration::create(string name,
                                                             string value,
                                                             string default_value) {

  shared_ptr<ConfigVariable> var = nullptr;

  auto it = this->variables.find(name);

  if (it != this->variables.end()) {

    var = it->second;
    var->setValue(value);

  } else {

    var = make_shared<StringConfigVariable>(name, value, default_value);
    this->variables.insert(make_pair(name, var));
  }

  return var;

}

std::shared_ptr<ConfigVariable> RuntimeConfiguration::set(string name, bool value) {

  shared_ptr<ConfigVariable> var = nullptr;

  auto it = this->variables.find(name);

  if (it != this->variables.end()) {

    var = it->second;
    var->setValue(value);

  } else {

    throw CPGBackupCtlFailure("unknown runtime variable: \"" + name + "\"");

  }

  return var;
}

std::shared_ptr<ConfigVariable> RuntimeConfiguration::set(string name, string value) {

  shared_ptr<ConfigVariable> var = nullptr;

  auto it = this->variables.find(name);

  if (it != this->variables.end()) {

    var = it->second;
    var->setValue(value);

  } else {

    throw CPGBackupCtlFailure("unknown runtime variable: \"" + name + "\"");

  }

  return var;

}

std::shared_ptr<ConfigVariable> RuntimeConfiguration::set(std::string name, int value) {

  shared_ptr<ConfigVariable> var = nullptr;

  /*
   * Check if the variable already exists. If true, assign
   * the new value and default_value.
   */
  auto it = this->variables.find(name);

  if (it != this->variables.end()) {

    var = it->second;

    /* set new value */
    var->setValue(value);

  } else {

    throw CPGBackupCtlFailure("unknown runtime variable: \"" + name + "\"");

  }

  return var;

}

