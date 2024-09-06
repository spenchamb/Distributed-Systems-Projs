from pymemcache.client.base import Client
import socket
import sys
import time

def send_request(host, port, cmd):
  client = Client(f"{host}:{port}")
  if cmd[0] == 'get':
    a = client.get(cmd[1][0]).decode() #PYMEMCACHE command!
    print(a)
  else:
    a = client.set(cmd[1][0], cmd[1][1]) #PYMEMCACHE command!
    if a: print('STORED')

if __name__ == "__main__":
    host = sys.argv[1]
    port = int(sys.argv[2])
    delay = int(sys.argv[3])
    
    send_request(host, port, ['set', ['key1',"Lorem ipsum dolor sit amet, aeque audiam animal ex sed. Pri ex everti facilis insolens, epicurei instructior eam ne, vel praesent sapientem et. Duo cu rebum porro delectus, usu an viris denique propriae, ad vide mentitum his. Pri eu idque soleat."]])
    
    if delay: time.sleep(1)
    send_request(host, port, ['set', ['key1',"Autem labitur interpretaris ex nam, an sed rebum indoctum. Et porro expetenda intellegat vix, ex utroque conclusionemque mea. Ne idque intellegebat vim, quod comprehensam et cum, ei copiosae efficiendi mea. Usu in nisl sadipscing consectetuer, ut omittantur persequeris vituperatoribus eam. Eos ut unum timeam insolens, ei usu dicant altera delicata."]])

    if delay: time.sleep(1)
    send_request(host, port, ['set', ['key1', "At quando dissentias cum, ne ludus quaeque instructior quo. Cu quo quis quaeque, odio nobis ius id, sit impedit maiorum epicuri in. Quem laudem ea per, elit dictas phaedrum usu at. An pro tempor nemore, eu sanctus atomorum adversarium duo, in agam convenire intellegam duo. Graece dignissim qui ut, eum cu commodo reprehendunt."]])

    if delay: time.sleep(1)
    send_request(host, port, ['set', ['key1', "Sed viris deserunt ei, per homero scripta no. An ius noster imperdiet, ut mel choro nominati, vidit disputationi per in. Illum fabellas qui te, debitis alienum cum eu. Ipsum inciderint ea has. Ad suas erant est."]])

    if delay: time.sleep(1)
    send_request(host, port, ['set', ['key1', "Nam pertinax neglegentur ea. Eum purto integre copiosae no. Odio splendide ne vix, in possit veritus deseruisse his. Eam inani expetendis ut, saperet legimus usu ne."]])
    
    if delay: time.sleep(1)
    
