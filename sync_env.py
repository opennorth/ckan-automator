#!/usr/bin/python
# -*- coding: utf-8 -*-
import ckanclientmtl
import ConfigParser 
import logging
import smtplib
import os


#Initial configuraiton: logger, config, etc.
#Open config file
config = ConfigParser.ConfigParser()
config.read('config.cfg')


logger = logging.getLogger('ckanclientmtl')
logger.setLevel(logging.INFO)

# add a file handler
logfinename = config.get('General', 'log_file')
logfile = logging.FileHandler(logfinename)
logfile.setLevel(logging.INFO)

# create a formatter and set the formatter for the handler.
frmt = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logfile.setFormatter(frmt)

# add the Handler to the logger
logger.addHandler(logfile)



#Get info for the "target" CKAN instance
ckan_target = config.get('General', 'ckan_target')
ckan_target_key = config.get('General', 'ckan_target_api_key')
ckan_version = config.get('General', 'ckan_version')

ckanclient = ckanclientmtl.CkanClientMtl(ckan_target, ckan_target_key, ckan_version)
ckanclient.logger = logger

ckansource = ckanclientmtl.CkanClientMtl('http://donnees.ville.montreal.qc.ca/')


'''Delete all groups of an install'''
#group_list = ckansource.get_group_list()
#ckansource.delete_groups(group_list)


'''Copy groups from another instance (ckansource) to another one (ckanclient)'''
#group_list = ckansource.get_group_list()
#ckanclient.push_groups(group_list)


'''Flushing all the packages of an instance'''
#package_list = ckanclient.get_package_list([u'accessibilite-universelle-blian', u'adresses-ponctuelles', u'affectation-du-sol', u'agriculture-urbaine-sondage', u'allocation-aux-partis-autorises', u'anciens-territoires', u'annuaire-statistique-agglo', u'arbres', u'arceaux-velos', u'arros-liste', u'art-public-information-sur-les-oeuvres-de-la-collection-municipale', u'batiments', u'batiments-certifies-qualite-famille', u'batiments-vacants-vm-2013-csv', u'bibliotheques-montreal-statistiques', u'budget', u'budget-rosemont-la-petite-patrie', u'calendrier-des-delais-de-conservation-des-documents-de-la-ville-de-montreal', u'calendrier-des-seances-riviere-des-prairies-pointe-aux-trembles', u'calendrier-instances-politiques-ville-centrale', u'calendrier-multiculturel', u'calendriers-ca-cdn-ndg-2013-2014', u'cameras-observation-routiere', u'camionnage-reglements', u'canopee', u'carte-postes-quartier', u'cartographie-numerique-de-base-1-1000-2d', u'casernes-pompiers', u'catalogue-bibliotheques', u'centres-hebergement-urgence', u'charte-montrealaise-des-droits-et-des-responsabilites', u'comite-executif-pv', u'commission-de-la-presidence-du-conseil', u'commission-de-la-securite-publique', u'commission-developpement-economique', u'commission-sur-l-eau-l-environnement-le-developpement-durable-et-les-grands-parcs', u'commission-sur-l-examen-des-contrats', u'commission-sur-la-culture-le-patrimoine-et-les-sports', u'commission-sur-le-developpement-social-et-la-diversite-sociale', u'commission-sur-les-finances-et-l-administration', u'commission-sur-les-transports-et-les-travaux-publics', u'commissions-permanentes-du-conseil-membres', u'comptage-vehicules-pietons', u'conditions-ski', u'conseil-agglo-pv', u'conseil-municipal-pv', u'contrats-25-000-et-plus-cdn-ndg', u'contrats-comite-executif', u'contrats-conseil-municipal-et-conseil-d-agglomeration', u'contrats-octroyes-par-les-fonctionnaires-agglomeration', u'contrats-octroyes-par-les-fonctionnaires-cote-des-neiges-notre-dame-de-grace-2014', u'contrats-octroyes-par-les-fonctionnaires-ville-centrale', u'coups-de-coeur-et-projets-culturels-de-l-evenement-montreal-engagee-pour-la-culture', u'deneigement', u'depenses-des-elus-autorisees-par-le-comite-executif', u'diagnostic-de-la-pratique-artistique-amateur-a-montreal', u'ecoterritoires', u'election-2013-resultats-au-poste-de-maire-de-la-ville-par-arrondissements', u'elections-2009-distribution-votes', u'elections-2009-districts-electoraux', u'elections-2009-postes-electifs', u'elections-2009-sections-vote', u'elections-2013-candidatures', u'elections-2013-districts-electoraux', u'elections-2013-postes-electifs', u'elections-2013-resultats-detailles', u'elections-2013-section-vote-par-adresse', u'em-cdn-ndg', u'equipements-collectifs-et-services-municipaux-ville-marie-2014', u'feux-malvoyants', u'feux-pietons', u'feux-tous', u'geobase', u'geolocalisation-des-bornes-fontaines', u'grands-parcs', u'grands-parcs-activites', u'guide-archives', u'http-ville-montreal-qc-ca-portal-page-pageid-8337-92865582-dad-portal-schema-portal-datedebut-2013', u'http-ville-montreal-qc-ca-portal-page-pageid-8337-92865582-dad-portal-schema-portal-datedebut-2014', u'hydrographie', u'info-collectes', u'info-travaux', u'inspection-aliments-contrevenants', u'jardins-communautaires', u'lieux-culturels', u'lieux-publics-climatises', u'limites-cours-eau', u'limites-terrestres', u'listes-des-elus-de-la-ville-de-montreal', u'listes-des-elus-du-conseil-d-agglomeration', u'maquette-numerique-batiments-citygml-lod2-avec-textures', u'matieres-residuelles-bilan-massique', u'membre-du-comite-executif-depuis-1986', u'membres-rlcm', u'montant-des-amendes-payees-et-impayees-par-bibliotheque', u'monuments', u'ombudsman', u'organigramme-cdn-ndg', u'palmares-des-documents-les-plus-empruntes', u'parcours-riverain', u'patinoires', u'patinoires-historique', u'pc-cdn-ndg', u'phototheque-archives', u'piscines-municipales', u'pistes-cyclables', u'plan-general-de-classification-des-documents-de-la-ville-de-montreal', u'plans-detailles-d-occupation-du-sol-de-la-ville-de-montreal-1949', u'plans-generaux-d-occupation-du-sol-de-la-ville-de-montreal-1949', u'points-eau', u'points-rencontres-livres-dans-la-rue', u'politique-de-consultation-et-de-participation-publique-de-la-ville-de-montreal', u'polygones-arrondissements', u'population-recensement', u'portrait-camps-de-jour2013', u'presence-des-elus-au-conseil-municipal', u'proces-verbaux-conseil-arrondissement-villeray-saint-michel-parc-extension-2013', u'proces-verbaux-conseil-arrondissement-villeray-saint-michel-parc-extension-2014', u'proces-verbaux-conseil-d-arrondissement-lasalle-2011', u'proces-verbaux-conseil-d-arrondissement-lasalle-2012', u'proces-verbaux-conseil-d-arrondissement-mercier-hochelaga-maisonneuve-2014', u'proces-verbaux-conseil-d-arrondissement-riviere-des-praires-pointe-aux-trembles', u'proces-verbaux-conseil-d-arrondissement-riviere-des-praires-pointe-aux-trembles-2011', u'proces-verbaux-conseil-d-arrondissement-riviere-des-prairies-pointe-aux-trembles-2013', u'proces-verbaux-conseil-d-arrondissement-riviere-des-prairies-pointe-aux-trembles-2014', u'proces-verbaux-conseil-d-arrondissement-ville-marie-2002-a-2014', u'programme-inclusion-innovation2008-2014', u'pv-ca-cdn-ndg-2013', u'pv-ca-cdn-ndg-2014', u'pv-ca-cdn-ndg-2015', u'quadrillage-sqrc', u'quartiers', u'quartiers-sociologiques', u'rapport-du-maire-cdn-ndg', u'rapport-du-maire-situation-financiere', u'rapport-fiancier-ville-marie-2004-2013', u'rapport-ouverture-donnees', u'recipiendaires-du-prix-paul-buissonneau', u'reconnaissance-panam', u'registre-dm-cdn-ndg', u'registre-pp-cdn-ndg', u'remboursement-des-frais-de-recherches-et-de-soutien', u'remuneration-elus', u'resultats-elections-2013', u'rpp-pv-ca-2002-2012', u'rpp-pv-ca-2013', u'rpp-pv-ca-2014', u'rsma-donnees-ruisso-annuelle', u'rsma-indicateur-qualo', u'rsma-points-d-echantillonnage-qualo', u'rsma-points-d-echantillonnage-ruisso', u'rsma-qualite-de-l-eau-en-rive-qualo', u'rsqa-indice-qualite-air', u'rsqa-liste-des-stations', u'rsqa-polluants-gazeux', u'rui', u'sites-hiver', u'sondage-satisfaction-citoyens', u'sondage-satisfaction-citoyens-2013', u'soutien-artistes-organismes-culturels', u'stationnement-rue', u'stationnement-sur-rue-signalisation-courant', u'stationnements-gratuits', u'subventions-conseil-de-ville-et-d-agglomeration', u'subventions-du-comite-executif', u'test-bvin', u'velos-comptage', u'vues-aeriennes-archives', u'vues-aeriennes-de-l-ile-de-montreal-1925-1935', u'vues-aeriennes-de-montreal-1958-1975', u'vues-aeriennes-obliques-de-l-ile-de-montreal-1960-1992', u'webdiffusion-seances-comite-executif-archives', u'webdiffusion-seances-conseil-municipal-archives'])
#ckanclient.delete_packages(package_list)

'''Copy packages from an instance (filtering a specific package)'''
source_packages = ckansource.get_package_list(["commission-de-la-securite-publique"])

'''Mapping from source group to target group'''

mapper = {	
	"environnement": "base-layer", 
	"securite-publique":"chtgroup", 
	"organisation-administration" : "base-layer",


}

for package in source_packages:
	for group in package["groups"]:
		group["name"] = mapper[group["name"]]


'''send packages to target instance'''
'''Set organization and licence according to ids of the organization and licence in the target environment'''
ckanclient.push_package_list(source_packages, push_resources=True, organization="my-test-org", licence="cc-by")
