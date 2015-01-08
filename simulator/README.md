#The Simulator
Note: the simulator is under active development and much of the information in this document will eventually change

##High-level overview

The general idea is that every module you want to control must "register with", "listen to", and "reply to" the *sim* module.  The *sim* module will manage the current simulation time and the order of execution of the modules.

The *sim* module will trigger the module with the next occuring event to run.  This module will run and report back its next event time.  The *sim* module will then update the simulation time and trigger the module with the next event. **lather, rinse, and repeat.**'

You can think of this module as a giant mutex that makes sure only one module is running at a time.  In addition, it includes features for passing the RDL around between modules, modules scheduling events for other modules, and adding the runtime of certain modules into the simulation time.

##Registering with the sim module

When the simulator starts up, it will send out a *sim.start* event.  Modules wishing to join the simulation should reply to this event with a *sim.join* request.  This *sim.join* request should include three pieces of information:

1. *mod_name* - A string containing the name of the module
2. *rank* - An int containing the rank that the module is running on
3. *next_event* - A double containing the time of the next event for that module
  * If the module does not have a "next event" to report, then this should be set to -1

If you are adding a new module, please note that there is a hard-coded number of modules that the *sim* module will wait for before beginning the simulation. You will need to increase this variable (*num_modules*) to make the *sim* module wait for module to join before starting. In future versions, I plan to eliminate this hardcoded value, but for now it serves my purposes.

If your module has a long start-up time and it misses the *sim.start* event, it can do two things:

1. Send a *sim.alive* request which will cause the *sim* module to resend the *sim.start* event
2. Send the *sim.join* anyway (I haven't tested that this works, but it should)

Again, sorry that this registration protocol isn't that polished. Eventually, I will go back and clean it up.

##Listening to the sim module

The *sim* module will send a *mod_name.trigger* request when it expects the module with the name *mod_name* to run.  This request includes a json version of the sim_state_t struct. There is a whole section below devoted to this struct.


You can call json_to_sim_state (in simulator.h) to deserialize the json into a sim_state_t struct.

##Replying to the sim module

After a module receives a trigger from the *sim* module, it should execute its next planned event and reply to the *sim* module with a *sim.reply* request.  This *sim.reply* request should be constructed as follows:

1. Call sim_state_to_json to create a JSON object with the sim_state serialized in it
2. Add to the JSON object a bool called *event_finished*
  * As I'm making this, I realize that this does absolutely nothing, but the simulator will crash if you don't include it.  I will look into removing this.

##Handling other callbacks

The simulator runs under the assumption that the *sim* module is the only thing triggering the execution of modules.  For modules like *sim_exec* and *sim_sched* which listen for events in the KVS, you should set the KVS callbacks to do the absolute minimum work possible and then insert the event into a queue for later processing once the module is triggered.
  
##The sim_state_t struct

###C version
1. *sim_time* - a double that holds the current simulation time
2. *timers* - a zhash_t that holds the next event timers for all the modules in the simulation
  * Key: a char* holding the module's name
  * Value: a double holding the time of the next event for that module
  * see sim_schedsrv.c for an example of how this is used
3. *rdl_string* - a serialized copy of the RDL

###JSON version

1. *sim_time* - the current sim_time
2. *event_timers* - a serialized copy of the zhash_t hold the event timers of all the modules in the simulation
3. *rdl* - a serialized copy of the rdl

##Examples
* submitsrv.c is an example of a super simple module that plugs into the simulator
  * It knows all of its event times a-priori, it does not depend on any other modules or other external events
* sim_execsrv.c is a more complex example
  * It deserializes the rdl_string from sim_state and uses that copy of the rdl
  * It relies on callbacks from requests from *sim_sched*, so these are queued up and executed after the module is triggered by the *sim* module
* sim_schedsrv.c is an even more complex example
  * It relies on callbacks from the KVS, so these are queued up and executed after the module is triggered by the *sim* module
  * It sets the *next_event* timer for another module, *sim_exec*.
  * It alters the *sim_time* by including the time it takes to execute the scheduling routine
  * It serializes its copy of the RDL into the sim_state