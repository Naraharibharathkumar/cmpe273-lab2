from spyne import Application, rpc, ServiceBase, Unicode, Iterable
from spyne.protocol.http import HttpRpc
from spyne.protocol.json import JsonDocument
from spyne.server.wsgi import WsgiApplication
import requests
import simplejson as json
import logging
import numpy
logging.basicConfig(level=logging.DEBUG)


class FilterCrimeReportService(ServiceBase):
    @rpc(Unicode, Unicode, Unicode, _returns=Iterable(Unicode))
    def checkcrime(ctx, lat, lon, radius):
        url = "https://api.spotcrime.com/crimes.json?lat=%s&lon=%s&radius=%s&key=." % (lat, lon, radius)
        get_response = requests.get(url)
        json_obj = json.loads(get_response.content)
        crime_count = 0;
        street_names = [];
        street_crimes = [];
        crime_type = {}
        final_streets = []
        event_time = [0,0,0,0,0,0,0,0]
        if get_response.status_code == 200:
            if len(json_obj['crimes']) != 0:
                for crime in json_obj['crimes']:
                    # Incrementing the crime count
                    crime_count += 1
                    # Checking the crime type and adding or incrementing based on IF
                    if crime['type'] not in crime_type:
                        crime_type[crime['type']] = 1
                    else:
                        crime_type[crime['type']] += 1
                    # Checking the address and adding or incrementing the streets & crimes in streets
                    if 'OF' in crime['address']:
                        street_name = crime['address'].split("OF ", 1)[1]
                        if street_name not in street_names:
                            street_names.append(street_name)
                            street_crimes.append(1)
                        else:
                            i = street_names.index(street_name)
                            street_crimes[i] += 1
                    if 'BLOCK BLOCK' in crime['address']:
                        street_name = crime['address'].split("BLOCK BLOCK ", 1)[1]
                        if street_name not in street_names:
                            street_names.append(street_name)
                            street_crimes.append(1)
                        else:
                            i = street_names.index(street_name)
                            street_crimes[i] += 1
                    if '&' in crime['address']:
                        street_name1 = crime['address'].split("& ", 1)[1]
                        street_name2 = crime['address'][:crime['address'].rfind(' &')]
                        if street_name1 not in street_names:
                            street_names.append(street_name1)
                            street_crimes.append(1)
                        else:
                            i = street_names.index(street_name1)
                            street_crimes[i] += 1
                        if street_name2 not in street_names:
                            street_names.append(street_name2)
                            street_crimes.append(1)
                        else:
                            i = street_names.index(street_name2)
                            street_crimes[i] += 1
                    # Checking the time and filtering the crimes based on given requirement
                    if 'AM' in crime['date']:
                        crime_hour = crime['date'].split(" ",1)[1]
                        crime_hour = crime_hour[:crime_hour.rfind(':')]
                        crime_minute = crime['date'].split(":",1)[1]
                        crime_minute = crime_minute[:crime_minute.rfind(' AM')]
                        c_hour = int(crime_hour)
                        c_min = int(crime_minute)
                        if c_hour < 4 or c_hour == 12:
                            if c_hour == 3 and c_min != 0:
                                event_time[1] += 1
                            else:
                                if c_hour == 12 and c_min == 0:
                                    event_time[7] += 1
                                else:
                                    event_time[0] += 1
                        if 3 < c_hour < 7:
                            if c_hour == 6 and c_min != 0:
                                event_time[2] += 1
                            else:
                                event_time[1] += 1
                        if 6 < c_hour < 10:
                            if c_hour == 9 and c_min != 0:
                                event_time[3] += 1
                            else:
                                event_time[2] += 1
                        if 9 < c_hour < 12:
                            event_time[3] += 1
                    else:
                        crime_hour = crime['date'].split(" ", 1)[1]
                        crime_hour = crime_hour[:crime_hour.rfind(':')]
                        crime_minute = crime['date'].split(":", 1)[1]
                        crime_minute = crime_minute[:crime_minute.rfind(' PM')]
                        c_hour = int(crime_hour)
                        c_min = int(crime_minute)
                        if c_hour < 4 or c_hour == 12:
                            if c_hour == 3 and c_min != 0:
                                event_time[5] += 1
                            else:
                                event_time[4] += 1
                        if 3 < c_hour < 7:
                            if c_hour == 6 and c_min != 0:
                                event_time[6] += 1
                            else:
                                event_time[5] += 1
                        if 6 < c_hour < 10:
                            if c_hour == 9 and c_min != 0:
                                event_time[7] += 1
                            else:
                                event_time[6] += 1
                        if 9 < c_hour < 12:
                            event_time[7] += 1
                # Finding the top 3 Dangerous streets
                arr_street = numpy.array(street_crimes)
                temp_street = numpy.argpartition(-arr_street, 3)
                top3_index = temp_street[:3]
                for j in top3_index:
                    final_streets.append(street_names[j])
                # Making json for event time count
                event_time_count = {"12:01am-3am" : event_time[0], "3:01am-6am" : event_time[1], "6:01am-9am" : event_time[2],
                                    "9:01am-12noon" : event_time[3], "12:01pm-3pm" : event_time[4], "3:01pm-6pm" : event_time[5],
                                    "6:01pm-9pm": event_time[6], "9:01pm - 12midnight" : event_time[7]}
                yield json.loads(json.dumps({ 'total_crime' : crime_count, 'the_most_dangerous_streets' : final_streets, 'crime_type_count' : crime_type, 'event_time_count' : event_time_count}))
            else:
                yield json.loads({'Message' : 'No Crimes reported at these area co-ordinates'})
        else:
            yield json.loads({'Message' : 'Bad co-ordinates, Try Again'})

app = Application([FilterCrimeReportService], tns='com.sjsu.cmpe273.hw', in_protocol=HttpRpc(validator='soft'), out_protocol=JsonDocument())

if __name__ == '__main__':
    from wsgiref.simple_server import make_server

wsgi_app = WsgiApplication(app)
server = make_server('0.0.0.0', 8000, wsgi_app)
server.serve_forever()