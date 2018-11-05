#include <rtconfig.hxx>

using namespace credativ;
using namespace std;

/* *****************************************************************************
 * ConfigVariable Base class
 * ****************************************************************************/

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


/* *****************************************************************************
 * BoolConfigVariable Runtime Variable Implementation
 * ****************************************************************************/

BoolConfigVariable::BoolConfigVariable(string name) {

  this->name = name;

}

BoolConfigVariable::BoolConfigVariable(string name,
                                       bool   value,
                                       bool   defaultval) : BoolConfigVariable(name) {

  this->value = value;
  this->default_value = defaultval;

}

void BoolConfigVariable::setValue(bool value) {
  this->value = value;
}

void BoolConfigVariable::setDefault(bool value) {

  this->default_value = value;

}

void BoolConfigVariable::getValue(bool &value) {
  value = this->value;
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

StringConfigVariable::StringConfigVariable(string name) : StringConfigVariable() {

  this->name = name;

}

StringConfigVariable::StringConfigVariable() {}

void StringConfigVariable::setValue(string value) {

  this->value = value;

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

void IntegerConfigVariable::getValue(int &value) {
  value = this->value;
}

void IntegerConfigVariable::check(int value) {


  if (this->enforce_rangecheck
      && (value > this->max && value < this->min)) {

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
  return oldval;

}

void IntegerConfigVariable::setValue(int value) {

  this->check(value);
  this->value = value;

}

void IntegerConfigVariable::setDefault(int defaultval) {

  this->check(defaultval);
  this->default_value = defaultval;

}

/* *****************************************************************************
 * Runtime Configuration Implementation
 * ****************************************************************************/

RuntimeConfiguration::RuntimeConfiguration() {

}

RuntimeConfiguration::~RuntimeConfiguration() {

}

ConfigVariable RuntimeConfiguration::get (string name) {

}

void RuntimeConfiguration::set(ConfigVariable variable) {

}
