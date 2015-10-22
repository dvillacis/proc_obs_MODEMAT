#/usr/bin/python
#Script Python para Asimilacion de Datos TEMP, METAR, SYNOP
#desde OGIMET.com
##Escrito por DAVID VILLACIS (david@villacis.net)
##Centro de Modelacion Matematica MODEMAT-EPN Quito-Ecuador
##http://www.modemat.epn.edu.ec

#Defino imports
import os
import pwd
import socket
import requests
from requests import ConnectionError
from requests import Timeout
from requests import HTTPError
from lxml import html
import datetime
import ConfigParser
import logging

### Importo archivo de configuracion
config = ConfigParser.RawConfigParser()
config.read('proc_obs_MODEMAT.config')

## Configuracion del Logger
LOG_FORMAT = '%(asctime)-15s %(message)s'
LOG_FILENAME = 'proc_obs_MODEMAT.log'

# set up logging to file - see previous section for more details
logging.basicConfig(level=logging.DEBUG,
                    format=LOG_FORMAT,
                    datefmt='%m-%d %H:%M',
                    filename=LOG_FILENAME,
                    filemode='w')
# define a Handler which writes INFO messages or higher to the sys.stderr
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
# set a format which is simpler for console use
formatter = logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s')
# tell the handler to use this format
console.setFormatter(formatter)
# add the handler to the root logger
logging.getLogger('').addHandler(console)


#Ejecuto limpieza de archivos temporales
logging.info("Main process: %s","Removing previously generated temporal files...")
remove_command = "rm -Rf " + config.get('IO','nombre_metar') + " " + config.get('IO','nombre_synop') + " " + config.get('IO','nombre_temp')
os.system(remove_command)
logging.info("Main process: %s", "Removed previously generated temporal files...")

#Cargo los archivos fuente a memoria
try:

	#Obtengo fecha actual
	now = datetime.datetime.now()
	fecha = "FECH:%s %s %s %s:00:00" % (config.get('FUENTE_DE_DATOS','data_ano_inicio'), config.get('FUENTE_DE_DATOS','data_mes_inicio'), config.get('FUENTE_DE_DATOS','data_dia_inicio'), now.hour)
	logging.info('Main process: %s', 'Setting date ' + fecha) 

	#Proceso METAR
	logging.info('Main process: %s', 'Processing METAR')
	out_file = open(config.get('IO','nombre_metar'),"w")
	out_file.write(fecha + "\n")

	parametros = {
		'estado': config.get('FUENTE_DE_DATOS','data_estado'),
		'fmt': config.get('FUENTE_DE_DATOS','data_formato'),
		'iord': config.get('FUENTE_DE_DATOS','data_iord')
	}

	q_url=config.get('FUENTE_DE_DATOS','data_url_metar')

	r = requests.get(q_url, params=parametros)
	tree = html.fromstring(r.text)
	info = tree.xpath('//pre/text()')
	
	i = 1
	for sonda in info:
		out_file.write(sonda[6:] + "\n")
		#print "Linea " + str(i) + ": " + repr(sonda)
		i = i + 1
	out_file.close()

	logging.info('Main process: %s', os.getcwd()+'/'+config.get('IO','nombre_metar') + ' wrote successfully!')

	#Proceso SYNOP
	logging.info("Main process: %s", "Processing SYNOP")
	out_file = open(config.get('IO','nombre_synop'),"w")
	out_file.write(fecha + "\n")

	parametros = {
		'estado': config.get('FUENTE_DE_DATOS','data_estado'),
		'tipo': config.get('FUENTE_DE_DATOS','data_tipo'),
		'ord': config.get('FUENTE_DE_DATOS','data_orden'),
		'nil': config.get('FUENTE_DE_DATOS','data_incluir_nulo'),
		'fmt': config.get('FUENTE_DE_DATOS','data_formato'),
		'ano': config.get('FUENTE_DE_DATOS','data_ano_inicio'),
		'mes': config.get('FUENTE_DE_DATOS','data_mes_inicio'),
		'day': config.get('FUENTE_DE_DATOS','data_dia_inicio'),
		'hora': str(now.hour),
		'anof': config.get('FUENTE_DE_DATOS','data_ano_fin'),
		'mesf': config.get('FUENTE_DE_DATOS','data_mes_fin'),
		'dayf': config.get('FUENTE_DE_DATOS','data_dia_fin'),
		'horaf': str(now.hour)
	}

	q_url=config.get('FUENTE_DE_DATOS','data_url_synops')
	r = requests.get(q_url, params=parametros)
	tree = html.fromstring(r.text)
	info = tree.xpath('//pre/text()')
	
	i = 1
	for sonda in info:
		out_file.write(sonda + "\n")
		#logging.info("Main process", "Linea " + str(i) + ": " + repr(sonda)
		i = i + 1
	out_file.close()

	logging.info('Main process: %s', os.getcwd()+'/'+config.get('IO','nombre_synop') + ' wrote successfully!')

	#Proceso TEMP
	logging.info("Main process: %s", "Processing TEMP")
	out_file = open(config.get('IO','nombre_temp'),"w")
	out_file.write(fecha + "\n")

	parametros = {
		'estado': config.get('FUENTE_DE_DATOS','data_estado'),
		'tipo': config.get('FUENTE_DE_DATOS','data_tipo'),
		'ord': config.get('FUENTE_DE_DATOS','data_orden'),
		'nil': config.get('FUENTE_DE_DATOS','data_incluir_nulo'),
		'fmt': config.get('FUENTE_DE_DATOS','data_formato'),
		'ano': config.get('FUENTE_DE_DATOS','data_ano_inicio'),
		'mes': config.get('FUENTE_DE_DATOS','data_mes_inicio'),
		'day': config.get('FUENTE_DE_DATOS','data_dia_inicio'),
		'hora': str(now.hour),
		'anof': config.get('FUENTE_DE_DATOS','data_ano_fin'),
		'mesf': config.get('FUENTE_DE_DATOS','data_mes_fin'),
		'dayf': config.get('FUENTE_DE_DATOS','data_dia_fin'),
		'horaf': str(now.hour)
	}

	q_url=config.get('FUENTE_DE_DATOS','data_url_upr')
	r = requests.get(q_url, params=parametros)
	tree = html.fromstring(r.text)
	info = tree.xpath('//pre/text()')
	
	i = 1
	for sonda in info:
		out_file.write(sonda + "\n")
		#print "Linea " + str(i) + ": " + repr(sonda)
		i = i + 1
	out_file.close()
	
	logging.info('Main process: %s', os.getcwd()+'/'+config.get('IO','nombre_temp') + ' wrote successfully!')

except ConnectionError as e:
	logging.error("ConnectionError: %s", e)
except HTTPError as e:
	logging.error("HTTPError: %s", e)
except Timeout as e:
	logging.error("Timeout: %s", e)


