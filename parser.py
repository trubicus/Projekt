#!/usr/bin/python
import paho.mqtt.client as mqtt
import sqlite3

# Global var {{{
host = "localhost"
port = 1883
keepalive = 60
base_topic = "devices/#"
client = None
database = None
messages = {}
# create_mqtt_sql {{{
create_mqtt_sql = """
                       create table mqtt(
                       name text,
                       online text,
                       localip text,
                       mac text primary key,
                       homie text,
                       signal text,
                       uptime text,
                       fwname text,
                       fwversion text,
                       implementation text,
                       config text,
                       ota_enabled text,
                       ota_status text
                       ); """
# }}}
# create_node_sql {{{
create_node_sql = """
                       create table node(
                       mac text,
                       node text,
                       type text,
                       properties text,
                       node_on text
                       );"""
#}}}
# }}}

def on_connect(client, userdata, flags, rc):  # {{{
    print("Connected!")
    client.subscribe(base_topic)
# }}}


def on_message(client, userdata, msg):  # {{{
    global database
    cursor = database.cursor()
    atribute = msg.topic.split('/')[-1]
    atribute_value = msg.payload.decode('UTF-8')
    messages[atribute] = atribute_value
    if atribute == "$type":
        node = msg.topic.split('/')[2]
        messages["node"] = node

    if (atribute == "$online" and
        list(cursor.execute("select count(*) from mqtt;"))[0][0] == 0):

        cursor.execute(  # {{{
                         """
                         insert into mqtt values (
                         '{}',
                         '{}',
                         '{}',
                         '{}',
                         '{}',
                         '{}',
                         '{}',
                         '{}',
                         '{}',
                         '{}',
                         '{}',
                         '{}',
                         '{}'
                         );
            """.format(
                messages["$name"],
                messages["$online"],
                messages["$localip"],
                messages["$mac"],
                messages["$homie"],
                messages["signal"],
                messages["uptime"],
                messages["name"],
                messages["version"],
                messages["$implementation"],
                messages["config"],
                messages["enabled"],
                messages["status"]
            )
                         )
        # }}}

        cursor.execute(  # {{{
                         """
                         insert into node values (
                         '{}',
                         '{}',
                         '{}',
                         '{}',
                         '{}'
                         );
            """.format(
                messages["$mac"],
                messages["node"],
                messages["$type"],
                messages["$properties"],
                messages["on"]
            )
                         )
        # }}}

    if(atribute == "uptime" and
         list(cursor.execute("select count(*) from mqtt;"))[0][0] == 1 and
         '$online' in messages.keys()):

# update_database {{{
        update_mqtt = """
                            update mqtt
                            set
                            name = '{0}',
                            online = '{1}',
                            localip = '{2}',
                            mac = '{3}',
                            homie = '{4}',
                            signal = '{5}',
                            uptime = '{6}',
                            fwname = '{7}',
                            fwversion = '{8}',
                            implementation = '{9}',
                            config = '{10}',
                            ota_enabled = '{11}',
                            ota_status = '{12}'
                            where mac = '{3}';
                            """.format(messages["$name"],
                                    messages["$online"],
                                    messages["$localip"],
                                    messages["$mac"],
                                    messages["$homie"],
                                    messages["signal"],
                                    messages["uptime"],
                                    messages["name"],
                                    messages["version"],
                                    messages["$implementation"],
                                    messages["config"],
                                    messages["enabled"],
                                    messages["status"]
                                    )
        update_node = """update node
                            set
                            mac = '{0}',
                            node = '{1}',
                            type = '{2}',
                            properties = '{3}',
                            node_on = '{4}'
                            where mac = '{0}';
                            """.format(
                                messages["$mac"],
                                messages["node"],
                                messages["$type"],
                                messages["$properties"],
                                messages["on"]
                            )

# }}}

        cursor.execute(update_mqtt)
        cursor.execute(update_node)

    database.commit()
# }}}


def check_database(base):  # {{{
    try:
        cursor = base.cursor()
        cursor.execute("select * from mqtt;")
    except sqlite3.OperationalError:
        cursor.execute(create_mqtt_sql)
        cursor.execute(create_node_sql)
        base.commit()
# }}}


def main():  # {{{
    global database
    database = sqlite3.connect("mqtt.db")
    check_database(database)

    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(host, port, keepalive)

    client.loop_forever()
# }}}


if __name__ == "__main__":
    main()
