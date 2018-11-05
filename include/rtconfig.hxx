#ifndef __HAVE_RUNTIME_CONFIG_VARIABLES__
#define __HAVE_RUNTIME_CONFIG_VARIABLES__

#include <common.hxx>
#include <unordered_set>
#include <unordered_map>

namespace credativ {

  /**
   * Type of ConfigVariable.
   */
  typedef enum {

    RT_CONFIG_VAR_BOOL,
    RT_CONFIG_VAR_STRING,
    RT_CONFIG_VAR_ENUM, /* always a vector of strings */
    RT_CONFIG_VAR_INTEGER,
    RT_CONFIG_VAR_UNKNOWN_TYPE

  } ConfigVariableType;

  /**
   * Base class for config runtime variables.
   */
  class ConfigVariable {
  protected:
    std::string name = "unknown";
  public:

    virtual ~ConfigVariable() {};

    virtual bool enforceRangeConstraint(bool force);

    virtual void setValue(std::string value);
    virtual void setValue(bool value);
    virtual void setValue(int value);

    virtual void setDefault(std::string defaultval);
    virtual void setDefault(bool defaultval);
    virtual void setDefault(int value);
    virtual void setRange(int min, int max);

    virtual void getValue(std::string &value);
    virtual void getValue(int &value);
    virtual void getValue(bool &value);

    virtual void getDefault(std::string &value);
    virtual void getDefault(int &value);
    virtual void getDefault(bool &value);
  };

  class BoolConfigVariable : public ConfigVariable {
  private:
    bool value = false;
    bool default_value = false;
  public:

    BoolConfigVariable(std::string name);
    BoolConfigVariable(std::string name, bool value, bool defaultval);
    virtual ~BoolConfigVariable() {};

    /**
     * Set runtime value
     */
    virtual void setValue(bool value);

    /**
     * Set default value
     */
    virtual void setDefault(bool value);

    /**
     * Stores the current assigned value in value.
     */
    virtual void getValue(bool &value);

    /**
     * Stores the current default value in defaultval.
     */
    virtual void getDefault(bool &value);

  };

  class StringConfigVariable : public ConfigVariable {
  private:

    std::string value = "";
    std::string default_value = "";

  public:

    StringConfigVariable();
    StringConfigVariable(std::string name);
    StringConfigVariable(std::string name,
                         std::string value,
                         std::string defaultval);
    virtual ~StringConfigVariable() {};

    /**
     * Set runtime value
     */
    virtual void setValue(std::string value);

    /**
     * Set default value.
     */
    virtual void setDefault(std::string defaultval);

    /*
     * Stores the current assign value in value.
     */
    virtual void getValue(std::string &value);

    /**
     * Stores the current default value in defaultval.
     */
    virtual void getDefault(std::string &value);

  };

  class EnumConfigVariable : public ConfigVariable {
  private:
    std::unordered_set<std::string> allowed_values;
    std::string value = "";
    std::string default_value = "";

    /**
     * Check the specified value if it's in the list
     * of allowed values. Throws a CPGBackupCtlFailure
     * in case it is not found.
     */
    void check_value(std::string value);
  public:

    EnumConfigVariable(std::string name,
                       std::unordered_set<std::string> possible_values);
    EnumConfigVariable(std::string name);
    EnumConfigVariable(std::string name,
                       std::string value,
                       std::string defaultval,
                       std::unordered_set<std::string> possible_values);
    virtual ~EnumConfigVariable() {};

    /**
     * Throws a CPGBackupCtlFailure
     * exception in case the value is rejected.
     */
    virtual void setValue(std::string value);

    /**
     * Sets the default value.
     *
     * NOTE: The caller need to initialize the list of
     *       possible values first, otherwise even
     *       the default value will be rejected.
     */
    virtual void setDefault(std::string defaultval);

    /**
     * Insert a string into the internal list
     * of allowed values.
     */
    void addAllowedValue(std::string value);

    /**
     * Stores the current value in value.
     */
    virtual void getValue(std::string &value);

    /**
     * Stores the current default value in defaultval.
     */
    virtual void getDefault(std::string &value);

  };

  class IntegerConfigVariable : public ConfigVariable {
  private:
    int value = 0;
    int default_value = 0;

    bool enforce_rangecheck = false;

    /*
     * allowed range for values
     */
    int min = 0;
    int max = 0;

    virtual void check(int value);

  public:

    IntegerConfigVariable(std::string name);
    IntegerConfigVariable(std::string name,
                          int value,
                          int defaultval,
                          bool enforceRangConstraint);
    virtual ~IntegerConfigVariable() {};

    /**
     * Turns range checks for configuration values
     * on or off.
     *
     * Returns the old setting to the caller.
     */
    virtual bool enforceRangeConstraint(bool force);

    /**
     * Sets the range of valid values.
     *
     * Only enforce if enforceRangeConstraint() was
     * called with true, turning range checks on.
     *
     * Throws CPGBackupCtlFailure if min is larger than max.
     */
    virtual void setRange(int min, int max);

    /**
     * Set value. If enforceRangeConstraint() was called
     * with true, internal range checks are performed, causing
     * setValue() to throw a CPGBackupCtlFailure exception.
     */
    virtual void setValue(int value);

    /**
     * Set default value. The default value is checked
     * against the allowed range of values, if enforceRangeConstraint()
     * was called with true before.
     */
    virtual void setDefault(int defaultval);

    /**
     * Stores the current value in value.
     */
    virtual void getValue(int &value);

    /**
     * Stores the current default value in defaultval.
     */
    virtual void getDefault(int &value);

  };

  class RuntimeConfiguration {
  protected:
    std::unordered_map<std::string, ConfigVariable> variables;
  public:
    RuntimeConfiguration();
    virtual ~RuntimeConfiguration();

    ConfigVariable get(std::string name);
    void set(ConfigVariable variable);

  };

}

#endif
