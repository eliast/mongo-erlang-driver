%%% -*- mode:erlang -*-
{application, mongodb,
 [
  % A quick description of the application.
  {description, "An Erlang driver for MongoDB"},

  % The version of the application. This is automatically populated by OTP Base
  {vsn, "0.0.1"},

  % All modules used by the application. This is automatically populated by OTP Base
  {modules,
   [
			mongodb, mongodb_app, mongodb_sup, mongodb_cursor, mongodb_bson, 
			mongodb_util, mongodb_collection, mongodb_conn, mongodb_cursor,
			mongodb_database, mongodb_cursor_api
   ]},

  % This is a list of the applications that your application depends on. This list must be filled out
  % carefully so that dependency resolution systems can function properly.
  {applications,
   [
    kernel,
    stdlib
   ]},

  % A list of the registered processes in your application.  Used to prevent collisions. 
  {registered, []},

  {mod, {mongodb_app, []}},
  {env, []}
 ]
}.
