#ifndef __HAVE_RUNTIME_CONFIG_VARIABLES__
#define __HAVE_RUNTIME_CONFIG_VARIABLES__

#include <common.hxx>
#include <unordered_set>
#include <unordered_map>

namespace pgbckctl {

  typedef void (*config_variable_assign_hook)(std::string val);

  /**
   * Base class for config runtime variables.
   */
  class ConfigVariable {
  protected:

    /* Identifier of a ConfigVariable object instance */
    std::string name = "unknown";

    /*
     * Assign hook function pointer. Only valid
     * if an assign function was specified by set_assign_hook()
     */
    config_variable_assign_hook assign_hook = NULL;

  public:

    virtual ~ConfigVariable() {};

    virtual bool enforceRangeConstraint(bool force);

    virtual std::string getName();

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

    virtual void set_assign_hook(config_variable_assign_hook ahook);

    /** Recalls the assign hook if available. */
    virtual void reassign();

    virtual void reset();
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
     * Returns the string represention of the current
     * bool value.
     */
    virtual void getValue(std::string &value);

    /**
     * Stores the current default value in defaultval.
     */
    virtual void getDefault(bool &value);

    /**
     * Reset current value back to its default.
     */
    virtual void reset();

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

    /**
     * Reset the configuration value back to
     * its default value.
     */
    virtual void reset();

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
    virtual void addAllowedValue(std::string value);

    /**
     * Stores the current value in value.
     */
    virtual void getValue(std::string &value);

    /**
     * Stores the current default value in defaultval.
     */
    virtual void getDefault(std::string &value);

    /**
     * Reset the configuration value back to
     * its default value.
     */
    virtual void reset();

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
                          bool enforceRangeConstraint = false);
    IntegerConfigVariable(std::string name,
                          int value,
                          int defaultval,
                          int range_min,
                          int range_max,
                          bool enforceRangeConstraint = true);
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
     * Only enforced if enforceRangeConstraint() was
     * called with true, turning range checks on.
     *
     * Throws CPGBackupCtlFailure if min is larger than max.
     *
     * NOTE: Changing a range does not revalidate current
     *       assigned value and default value!
     *       To recheck current assigned values, the caller
     *       should turn off range checks and turn it on, which
     *       will revalidate the settings!
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
     * Returns the string represention if the current value.
     */
    virtual void getValue(std::string &value);

    /**
     * Stores the current default value in defaultval.
     */
    virtual void getDefault(int &value);

    /**
     * Reset the configuration value back to
     * its default value.
     */
    virtual void reset();

  };

  /**
   * Config variable iterator.
   */
  typedef std::unordered_map<std::string,
                             std::shared_ptr<ConfigVariable>>::const_iterator config_variable_iterator;

  /**
   * Runtime configuration class, encapsulates access
   * to configuration variables used, set and updated
   * during runtime.
   *
   * Since a runtime configuration variables must be accessible
   * globally, every ConfigVariable instance is managed as a shared
   * pointer internally. This means, that if copies are kept anywhere
   * and are set, updated, those changes are visible through
   * any layer using the same reference to a runtime configuration.
   *
   * Classes which depend on those settings globally should inherit from
   * the RuntimeVariableEnvironment base class.
   *
   * RuntimeConfiguration and ancestors are also used in various
   * places where we want to carry specific options to actions around,
   * see src/catalog/output.cxx for an example.
   */
  class RuntimeConfiguration {
  protected:
    std::unordered_map<std::string, std::shared_ptr<ConfigVariable>> variables;
  public:

    RuntimeConfiguration();
    virtual ~RuntimeConfiguration();

    virtual std::shared_ptr<ConfigVariable> get(std::string name);

    virtual std::shared_ptr<ConfigVariable> set(std::string name, int value);
    virtual std::shared_ptr<ConfigVariable> set(std::string name,
                                                std::string value);
    virtual std::shared_ptr<ConfigVariable> set(std::string name,
                                                bool value);

    virtual std::shared_ptr<ConfigVariable> create(std::string name, int value, int default_value);
    virtual std::shared_ptr<ConfigVariable> create(std::string name, int value, int default_value,
                                                   int range_min, int range_max);
    virtual std::shared_ptr<ConfigVariable> create(std::string name,
                                                   std::string value,
                                                   std::string default_value,
                                                   std::unordered_set<std::string> possible_values);
    virtual std::shared_ptr<ConfigVariable> create(std::string name,
                                                   std::string value,
                                                   std::string default_value);
    virtual std::shared_ptr<ConfigVariable> create(std::string name,
                                                   bool value,
                                                   bool default_value);

    virtual config_variable_iterator begin();
    virtual config_variable_iterator end();

    virtual void reset(std::string name);

    virtual size_t count_variables();

  };


  /**
   * Base interface for classes using runtime configurations.
   *
   * This is a shell class, transporting references to runtime
   * object instances. Usually they aren't instantiated
   * by this shell class itself, but are created and assigned
   * from a single caller, since those objects have a global
   * visibility.
   */
  class RuntimeVariableEnvironment {
  protected:
    std::shared_ptr<RuntimeConfiguration> runtime_config = nullptr;
  public:

    RuntimeVariableEnvironment() {};
    RuntimeVariableEnvironment(std::shared_ptr<RuntimeConfiguration>);
    virtual ~RuntimeVariableEnvironment() {};

    /**
     * Factory method
     */
    static std::shared_ptr<RuntimeConfiguration> createRuntimeConfiguration();

    /*
     * Returns the current runtime configuration instance.
     */
    virtual std::shared_ptr<RuntimeConfiguration> getRuntimeConfiguration();

    /*
     * Assigns a new runtime configuration instance.
     */
    virtual void assignRuntimeConfiguration(std::shared_ptr<RuntimeConfiguration> rtc);

  };

}

#endif
