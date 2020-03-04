#!/usr/bin/env python3
# -*- coding: utf8 -*-

"""
    Date de création: 2020-02-29
    Date de mise à jour: 2020-03-04
    Auteur: Jean-François Bourdon (https://github.com/jfbourdon/)
    
    Description:
        Permet l'extraction des coordonnées de repères géodésiques
        de type planimétrique à partir des fiches PDF disponibles
        sur un serveur du MERN.
        
        À noter que le script actuel n'est pas particulièrement robuste
        et qu'il y a de fortes chances qu'il ne réussisse pas à extraire
        les coordonnées de toutes les fiches. Une validation manuelle
        des données extraire est avisée.
        
        Partie de code sur le multiprocessing tirée de
        https://stackoverflow.com/a/13530258
        
        Le multiprocessing n'est peut-être pas la meilleure approche,
        du multithreading serait peut-être mieux, mais ma première
        tentative a complètement rempli ma RAM.
    
    Paramètres:
        - path_ls_matricule : chemin d'accès au fichier TXT contenant la liste
                              des matricules à télécharger (un matricule par ligne)
        - path_data : chemin d'accès au fichier TXT qui contiendra les données
                      (écrase le fichier si déjà existant)
        - outdir : chemin d'accès au répertoire où seront enregistrés les fiches
                   au format PDF et TXT
        - path_pdf2txt : chemin d'accès au script pdf2txt.py de PDFMiner
    
    Sortie:
        Fichier TXT ayant une ligne pour chaque matricule avec les informations suivantes:
          - Numéro de matricule
          - Fuseau MTM
          - Coordonnée Y (m)
          - Coordonnée X (m)
          - Altitude orthométrique CGVD28 (m)
          - Note si erreur
"""


import os
import re
import sys
import subprocess
import multiprocessing as mp

import requests
import pdfminer


def float2str(x):
    return x.replace(" ","").replace(",",".")

def is_number(x):
    try:
        float(x)
        return True
    except ValueError:
        return False

def extract_fxyz(path_txt):
    matricule_repere = os.path.basename(path_txt)[:-4]
    
    # Extraction des lignes pertinentes de la fiche
    with open(path_txt, "r", encoding="utf-8") as f:
        ls_lines = f.readlines()
        ls_lines = [line.rstrip() for line in ls_lines]
    
    # Index de début du tableau
    try:
        index_tbl = ls_lines.index("Coordonnées") + 1
        valid = True
    except:
        valid = False
    
    if valid:
        index_latitude = ls_lines.index(" Latitude/y (m)")
        index_longitude = ls_lines.index(" Longitude/x (m)")
        
        nb_line_tbl = ls_lines[index_tbl:].index("")
        index_scopq_tbl = ls_lines[index_tbl:].index("SCOPQ")
        
        try:
            index_recouv_tbl = ls_lines[index_tbl:].index("Recouv.")
            recouvrement = True
        except:
            recouvrement = False
        
        # Altitude orthométrique
        try:
            index_alt = ls_lines.index("Altitude orthométrique (m) :")
            note_alt = " "
            if is_number(float2str(ls_lines[index_alt + 1])):
                scopq_alt = float2str(ls_lines[index_alt + 1])
            elif is_number(float2str(ls_lines[index_alt + 1])):
                scopq_alt = float2str(ls_lines[index_alt - 1])
            else:
                scopq_alt = " "
                note_alt = "Altitude existante, mais introuvable"
        except:
            scopq_alt = " "
            note_alt = "Aucune altitude"
        
        # SCOPQ -> Fuseau, Y, X
        scopq_fus0 =           ls_lines[index_tbl + nb_line_tbl + index_scopq_tbl - 1]
        scopq_y0   = float2str(ls_lines[index_latitude + index_scopq_tbl])
        scopq_x0   = float2str(ls_lines[index_longitude + index_scopq_tbl])
        
        ls_data = ["\t".join([matricule_repere, scopq_fus0, scopq_y0, scopq_x0, scopq_alt, note_alt + "\n"])]
        
        # Recouvrement -> Fuseau, Y, X
        if recouvrement:
            scopq_fus1 =           ls_lines[index_tbl + nb_line_tbl + index_recouv_tbl - 1]
            scopq_y1   = float2str(ls_lines[index_latitude + index_recouv_tbl])
            scopq_x1   = float2str(ls_lines[index_longitude + index_recouv_tbl])
            
            ls_data.append("\t".join([matricule_repere, scopq_fus1, scopq_y1, scopq_x1, scopq_alt, note_alt + "\n"]))
    else:
        ls_data = ["\t".join([matricule_repere, " ", " ", " ", " ", "Aucunes coordonnées\n"])]
    
    return ls_data

def download_data(matricule_repere, path_pdf2txt, outdir):    
    os.makedirs(outdir, exist_ok=True)
    
    # Téléchargement de la fiche PDF
    full_url = "https://fichegeodesique.mern.gouv.qc.ca/matricule-datum/" + matricule_repere + "/1"
    path_pdf = os.path.join(outdir, matricule_repere + ".pdf")
    r = requests.get(full_url, allow_redirects=True)
    open(path_pdf, 'wb').write(r.content)
    
    # Conversion de la fiche en fichier TXT
    path_txt = os.path.join(outdir, matricule_repere + ".txt")
    cmd = " ".join(["py -3", path_pdf2txt,
                          "-c utf-8",
                          "-t text",
                          "-o", path_txt,
                          path_pdf])
    subprocess.run(cmd)

def worker(ii, matricule_repere, path_data, outdir, path_pdf2txt, nb_tot, q):
    try:
        download_data(matricule_repere, path_pdf2txt, outdir)
        ls_data = extract_fxyz(os.path.join(outdir, matricule_repere + ".txt"))
    except:
        ls_data = "\t".join([matricule_repere, " ", " ", " ", " ", "Échec du téléchargement\n"])
    
    with open(path_data, "a", encoding="utf-8") as f:
        f.writelines(ls_data)
    
    width = len(str(nb_tot))
    print(f"({ii:{width}}/{nb_tot}) ==> {matricule_repere}")
    q.put(ls_data)
    return ls_data

def listener(q):
    '''listens for messages on the q, writes to file. '''

    with open(path_data, 'w') as f:
        while 1:
            m = q.get()
            if m == 'kill':
                f.write('killed')
                break
            f.writelines(ls_data)
            f.flush()

def main(ls_zipped, path_data, outdir, path_pdf2txt, nb_tot):
    #must use Manager queue here, or will not work
    manager = mp.Manager()
    q = manager.Queue()
    pool = mp.Pool(mp.cpu_count() - 1)

    #put listener to work first
    watcher = pool.apply_async(listener, (q,))

    #fire off workers
    jobs = []
    for ii, matricule_repere in ls_zipped:
        job = pool.apply_async(worker, (ii, matricule_repere, path_data, outdir, path_pdf2txt, nb_tot, q))
        jobs.append(job)

    # collect results from the workers through the pool result queue
    for job in jobs: 
        job.get()

    #now we are done, kill the listener
    q.put('kill')
    pool.close()
    pool.join()

if __name__ == "__main__":
   path_ls_matricule = sys.argv[1]
   path_data = sys.argv[2]
   outdir = sys.argv[3]
   path_pdf2txt = sys.argv[4]
   
   # Lecture du fichier contenant la liste des matricules
   with open(path_ls_matricule, "r", encoding="utf-8") as f:
       ls_lines = f.readlines()
       ls_lines = [line.rstrip() for line in ls_lines]
   
   # Écriture du fichier qui contiendra les données
   with open(path_data, "w", encoding="utf-8") as f:
       f.write("\t".join(["matricule_repere", "fuseau", "y", "x", "z", "Note\n"]))
   
   # Réarangement de la liste de matricules à traiter
   nb_tot = len(ls_lines)
   ls_items = range(1, nb_tot+1)
   ls_zipped = list(zip(ls_items, ls_lines))
   
   main(ls_zipped, path_data, outdir, path_pdf2txt, nb_tot)
