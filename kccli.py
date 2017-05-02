#!/usr/bin/env python

import argparse
import requests
import json

rest_host = 'localhost'
rest_port = {{connect_rest_port}}
rest_url = 'http://%s:%d' % (rest_host, rest_port)


class Error(Exception):
    pass


class ConnectorError(Error):
    def __init__(self, connector):
        self.connector = connector


class PluginError(Error):
    def __init__(self, plugin):
        self.plugin = plugin


class InvalidAction(ConnectorError):
    def __init__(self, action):
        self.action = action


class UnknownConnector(ConnectorError):
    def __init__(self, connector):
        self.connector = connector


class UnknownPlugin(PluginError):
    def __init__(self, plugin):
        self.plugin = plugin


class JsonError(Error):
    def __init__(self, json_data):
        self.json_data = json_data


class MissingKey(JsonError):
    def __init__(self, key):
        self.key = key


def get_arguments():
    parser = argparse.ArgumentParser(description='Admin script for kafka-connect')
    parser.add_argument('--list-connectors', '-l', action='store_true', help='Return a list of all connectors')
    parser.add_argument('--list-tasks', '-t', action='store_true', help='Return a list of current tasks for connector')
    parser.add_argument('--list-plugins', '-p', action='store_true', help='Return a list of installed plugins')
    parser.add_argument('--connector', '-c', dest='connector', default='all',
                        help='Connector name (default %(default)s)')
    parser.add_argument('--config', '-v', action='store_true', help='Return the configuration of a connector')
    parser.add_argument('--create', '-C', action='store_true', help='Create a connector')
    parser.add_argument('--json-file', '-J', help='JSON file to parse')
    parser.add_argument('--validate', '-V', action='store_true', help='Validate JSON file for proper fields and format')
    parser.add_argument('--status', action='store_true', help='Return the status of a connector')
    parser.add_argument('--pause', action='store_true', help='Pause a connector')
    parser.add_argument('--resume', action='store_true', help='Resume a connector')
    parser.add_argument('--restart', action='store_true', help='Restart a connector')
    return parser.parse_args()


def does_it_exist(connector):
    end_point = '%s/%s/%s' % (rest_url, "connectors", connector)
    response = requests.get(end_point)
    if response.status_code != 200:
        raise UnknownConnector(connector)
    else:
        return True


def get_all_connectors():
    end_point = '%s/%s' % (rest_url, "connectors")
    response = requests.get(end_point)
    all_connectors = response.json()
    return all_connectors


def get_connector_config(connector):
    try:
        does_it_exist(connector)
    except UnknownConnector as e:
        print "Connector %s not found" % e.connector
    else:
        end_point = '%s/%s/%s/%s' % (rest_url, "connectors", connector, "config")
        response = requests.get(end_point)
        connector_config = response.json()
        return connector_config


def print_connector_config(connector):
    config = get_connector_config(connector)
    print json.dumps(config, indent=4)


def get_tasks(connector):
    try:
        does_it_exist(connector)
    except UnknownConnector as e:
        print "Connector %s not found" % e.connector
    else:
        end_point = '%s/%s/%s/%s' % (rest_url, "connectors", connector, "tasks")
        response = requests.get(end_point)
        connector_tasks = response.json()
        return connector_tasks


def connector_action(connector, action):
    if action not in ['pause', 'resume', 'restart']:
        raise ("invalid action", action)
    try:
        does_it_exist(connector)
    except UnknownConnector as e:
        print "Connector %s not found" % e.connector
    else:
        end_point = '%s/%s/%s/%s' % (rest_url, "connectors", connector, action)
        if action in ['pause', 'resume']:
            response = requests.put(end_point)
            if response.status_code == 202:
                print "%s %sd" % (connector, action)
            else:
                print "Error: %i - %s" % (response.status_code, response.reason)
        elif action == "restart":
            response = requests.post(end_point)
            if response.status_code == 200:
                print "%s restarted" % connector
            else:
                print "Error: %i - %s" % (response.status_code, response.reason)


def create_connector():
    def ask_splunk_source_questions():
        pass

    def ask_splunk_sink_questions():
        ssl = raw_input("SSL enabled [Y/n]? ") or 'Y'
        if ssl.lower() == 'y':
            input['config']['splunk.ssl.enabled'] = 'true'
        elif ssl.lower() == 'n':
            input['config']['splunk.ssl.enabled'] = 'false'
        else:
            print "Invalid selection"
            ask_splunk_sink_questions()
        svc = raw_input("Validate SSL certificates [Y/n]? ") or 'Y'
        if svc.lower() == 'y':
            input['config']['splunk.ssl.validate.certs'] = 'true'
        elif svc.lower() == 'n':
            input['config']['splunk.ssl.validate.certs'] = 'false'
        else:
            print "Invalid selection"
            ask_splunk_sink_questions()
        input['config']['splunk.remote.host'] = raw_input("Splunk remote host [localhost]: ") or 'localhost'
        input['config']['splunk.remote.port'] = raw_input("Splunk remote port [8088]: ") or '8088'
        input['config']['splunk.auth.token'] = raw_input("Splunk token: ")
        input['config']['tasks.max'] = raw_input("Max tasks [5]: ") or '5'
        input['config']['topics'] = raw_input("Topic to consume: ")
        return input

    def ask_file_source_questions():
        pass

    def ask_splunk_sink_questions():
        pass

    try:
        config_data = load_json_file(args.json_file)
        try:
            does_it_exist(config_data['name'])
        except UnknownConnector:
            end_point = '%s/%s' % (rest_url, "connectors")
            response = requests.post(end_point, json=config_data)
            if response.status_code != 201:
                print "Error: %i - %s" % (response.status_code, response.reason)
                return False
            else:
                print "Connector %s created:" % config_data['name']
                print_connector_config(config_data['name'])
        else:
            print "Error: %s already exists" % connector
    except (IOError, TypeError):
        # Menuize
        data = {'config': {}}
        # Get onnector name
        print "Name for new connector"
        data['name'] = raw_input(" >> ")
        try:
            does_it_exist(data['name'])
        except UnknownConnector:
            plugins = get_all_plugins()
            n = 1
            menu = {}
            for p in plugins:
                menu[str(n)] = p
                n += 1
            print "Select Connector Plugin:"
            for p in menu.keys():
                print "%s. %s" % (p, menu[p])
            choice = raw_input(" >> ")
            data['config']['connector.class'] = menu[str(choice)]

            if data['config']['connector.class'] == 'io.confluent.kafka.connect.splunk.SplunkHttpSourceConnector':
                data.update(ask_splunk_source_questions())
            elif data['config']['connector.class'] == 'io.confluent.kafka.connect.splunk.SplunkHttpSinkConnector':
                data.update(ask_splunk_sink_questions())
            elif data['config']['connector.class'] == 'org.apache.kafka.connect.file.FileStreamSourceConnector':
                data.update(ask_file_source_questions())
            elif data['config']['connector.class'] == 'org.apache.kafka.connect.file.FileStreamSinkConnector':
                data.update(ask_file_sink_questions())
            else:
                raise UnknownPlugin
            print data
        else:
            print "Error: %s already exists" % connector


def load_json_file(json_file):
    try:
        with open(json_file) as jf:
            json_data = json.load(jf)
    except ValueError:
        print "Error: Invalid JSON in %s" % json_file

    try:
        validate_json_keys(json_data)
        return json_data
    except KeyError:
        print "Couldn't validate JSON keys"


def validate_json_keys(json_data):
    # Required top-level keys
    required_keys = ['name', 'config']
    for key in required_keys:
        if not json_data[key]:
            print "Error: JSON data missing required key: %s" % key
            return 1
    # TODO - update with required keys for other connectors
    # Keys required for the Splunk sink connector
    required_splunk_sink_keys = ['splunk.remote.port',
                                 'splunk.ssl.enabled',
                                 'topics',
                                 'tasks.max',
                                 'connector.class',
                                 'splunk.ssl.validate.certs',
                                 'splunk.remote.host',
                                 'splunk.auth.token']
    # Keys required for the file sink connector
    required_file_sink_keys = ['connector.class',
                               'tasks.max',
                               'file',
                               'topics']
    # Validate config keys
    try:
        if json_data['config']['connector.class'] == 'io.confluent.kafka.connect.splunk.SplunkHttpSinkConnector':
            required_config_keys = required_splunk_sink_keys
        elif json_data['config']['connector.class'] == 'FileStreamSink':
            required_config_keys = required_file_sink_keys
        else:
            print "Error: Invalid connector class: %s" % json_data['config']['connector.class']
    except KeyError:
        print "Error: connector.class key missing"
    else:
        for key in required_config_keys:
            try:
                X = json_data['config'][key]
            except KeyError:
                print "Error: JSON data missing required key: %s" % key
                raise
        return True


def delete_connector(connector):
    try:
        does_it_exist(connector)
    except UnknownConnector as e:
        print "Connector %s not found" % e.connector
    else:
        end_point = '%s/%s/%s' % (rest_url, "connectors", connector)
        response = requests.delete(end_point)
        if response.status_code == 201:
            print "Connector %s deleted" % connector
        else:
            print "Error: %i - %s" % (response.status_code, response.reason)


def config_connector(connector):
    pass


def get_all_plugins():
    plugins = []
    end_point = '%s/%s' % (rest_url, "connector-plugins")
    response = requests.get(end_point)
    if response.status_code == 200:
        for key in response.json():
            plugins.append(key['class'])
        return plugins
    else:
        print "Error: %i - %s" % (response.status_code, response.reason)
        return False


def main(args):
    if args.list_connectors:
        print json.dumps(get_all_connectors(), indent=4)

    elif args.list_plugins:
        print json.dumps(get_all_plugins(), indent=4)

    elif args.list_tasks:
        if args.connector == "all":
            conns = get_all_connectors()
            for c in conns:
                print json.dumps(get_tasks(c), indent=4)
        else:
            print json.dumps(get_tasks(args.connector), indent=4)

    elif args.create:
        create_connector()

    elif args.config:
        if args.connector == "all":
            conns = get_all_connectors()
            for c in conns:
                print_connector_config(c)
        else:
            print_connector_config(args.connector)

    elif args.status:
        if args.connector == "all":
            conns = get_all_connectors()
            for c in conns:
                get_connector_status(c)
        else:
            print json.dumps(get_connector_status(args.connector), indent=4)

    elif args.pause:
        if args.connector == "all":
            conns = get_all_connectors()
            for c in conns:
                connector_action(c, "pause")
        else:
            connector_action(args.connector, "pause")

    elif args.resume:
        if args.connector == "all":
            conns = get_all_connectors()
            for c in conns:
                connector_action(c, "resume")
        else:
            connector_action(args.connector, "resume")

    elif args.restart:
        if args.connector == "all":
            conns = get_all_connectors()
            for c in conns:
                connector_action(c, "restart")
        else:
            connector_action(args.connector, "restart")


if __name__ == "__main__":
    try:
        args = get_arguments()
        main(args)
    except(Exception):
        raise
    else:
        pass
    finally:
        pass
