import json
import joblib
import numpy as np
import pandas as pd
import re
import uuid
import datetime


def date_difference(date_time_str_start, date_time_str_end):
    # date_time_str = '2018-06-29'
    # date_time_str1 = '2018-06-30'

    date_time_start = datetime.datetime.strptime(date_time_str_start.split('T')[0], '%Y-%m-%d')
    date_time_end = datetime.datetime.strptime(date_time_str_end.split('T')[0], '%Y-%m-%d')
    if date_time_start == date_time_end:
        return 1
    else: 
        return int(str(date_time_end - date_time_start).split(' ')[0]) + 1

def user_recovery_features(gdpr_id, df_prestazioni_all):    
    #gdpr_id= 213512642
    ricovero_id = []
    guid= uuid.uuid4()
    df_user_pr = df_prestazioni_all[df_prestazioni_all.gdpr_id == gdpr_id]
    for dd in df_user_pr.datediff:
        if dd > 10:
            guid= uuid.uuid4()
        ricovero_id.append(guid)


    df_user_pr['ricovero_id'] = ricovero_id

    n_ricoveri = len(df_user_pr.ricovero_id.unique())   #return
    #print(n_ricoveri)
    if n_ricoveri!=0:
        days=[]
        for ric in df_user_pr.ricovero_id:
            df_user_ric = df_user_pr[df_user_pr.ricovero_id == ric]
            #print(df_user_ric)
            dstart = df_user_ric['data_erogazione'].head(1).values[0]
            #print(dstart)
            dend= df_user_ric.data_erogazione.tail(1).values[0]
            #print(dstart)
            days_ric = date_difference(str(dstart), str(dend))
            #print(days_ric)
            days.append(days_ric)
        df_user_pr['days'] = days
        #print(df_user_pr.days)
        if len(df_user_pr.days.unique()) > 1:
            n_days_tot_rec = df_user_pr.days.unique().sum() # return

            max_days_rec = df_user_pr.days.unique().max() # return

            mean_days_rec = df_user_pr.days.unique().mean() # return
        else:
            n_days_tot_rec= df_user_pr.days.unique().sum()
            max_days_rec= df_user_pr.days.values[0]
            mean_days_rec= df_user_pr.days.values[0]
    else:
        n_days_tot_rec=0
        max_days_rec= 0
        mean_days_rec= 0

    ret = pd.DataFrame(data = {'gdpr_id':[gdpr_id], 'n_ricoveri': [n_ricoveri], 
                               'n_days_tot_rec': [n_days_tot_rec], 'max_days_rec': [max_days_rec], 
                               'mean_days_rec': [mean_days_rec]})
    return ret

def dummifier(final_dataframe):

    features_list = ['RAC3_tot', 'RAC3_ok', 'RAC3_low', 'RAC3_hight', 'RAC3_trend',
       'TRIG_tot', 'TRIG_ok', 'TRIG_low', 'TRIG_hight', 'TRIG_problem',
       'TRIG_trend', 'LDL_tot', 'LDL_ok', 'LDL_low', 'LDL_hight', 'LDL_trend',
       'MICA24_tot', 'MICA24_ok', 'MICA24_low', 'MICA24_hight',
       'MICA24_problem', 'MICA24_trend', 'GLUM_tot', 'GLUM_ok', 'GLUM_low',
       'GLUM_hight', 'GLUM_trend', 'PALB_tot', 'PALB_ok', 'PALB_low',
       'PALB_trend', 'B2GG_tot', 'B2GG_ok', 'B2GG_low', 'B2GG_hight',
       'B2GG_trend', 'B2GM_tot', 'B2GM_ok', 'B2GM_low', 'B2GM_hight',
       'B2GM_trend', 'CG3_tot', 'CG3_ok', 'CG3_low', 'CG3_hight', 'CG3_trend',
       'CG_tot', 'CG_ok', 'CG_hight', 'CG_trend', 'CI_tot', 'CI_ok', 'CI_low',
       'CI_hight', 'CI_trend', 'CGM_tot', 'CGM_ok', 'CGM_hight', 'CGM_trend',
       'GLU24_tot', 'GLU24_problem', 'CACL1_tot', 'CACL1_hight', 'CACL1_trend',
       'eta', 'perc_PS', 'perc_ambulatorio', 'perc_ricoverato',
       'num_prestazioni_tot', 'n_ricoveri', 'n_days_tot_rec', 'max_days_rec',
       'mean_days_rec', 'sesso_F', 'sesso_M', 'decesso_False', 'decesso_True']

    input_dataset = pd.DataFrame(columns = features_list)

    for column in features_list:
        try:
            input_dataset[column] = fe[column]
        except:
            input_dataset[column] = -99

    if fe.loc[0, 'sesso'] == 'M':
        input_dataset['sesso_M'] = 1
        input_dataset['sesso_F'] = 0
    else:
        input_dataset['sesso_M'] = 0
        input_dataset['sesso_F'] = 1

    if fe.loc[0, 'decesso'] == 'False':
        input_dataset['decesso_False'] = 1
        input_dataset['decesso_True'] = 0
    else:
        input_dataset['decesso_False'] = 0
        input_dataset['decesso_True'] = 1

    raw_data = input_dataset.fillna(value = 0, inplace = False).values.tolist()

    return raw_data

def main(df_prestazioni,df_risultati_lab,df_master):
    users =  df_prestazioni.gdpr_id.unique()
###################### CALCOLO LA COLONNA DATEDIFF  #############################################
    df_prestazioni_all = pd.DataFrame()
    for user in users:
        df_user = df_prestazioni[df_prestazioni.gdpr_id == user ].sort_values(by=['data_erogazione'])
        df_user_ricov = df_user[df_user.tipoprestazione == 'ricoverato']
        df_user_ricov.loc[:, 'data_erogazione'] = pd.to_datetime(df_user_ricov['data_erogazione'])
        df_user_ricov.loc[:, 'datediff'] = df_user_ricov.data_erogazione.diff().astype(str)
        df_user_ricov.loc[:, 'datediff'] = df_user_ricov['datediff'].apply(lambda x : x.split(' ')[0])
        df_user_ricov.loc[:, 'datediff'] = df_user_ricov['datediff'].replace('NaT','0')
        df_user_ricov.loc[:, 'datediff']  = pd.to_numeric(df_user_ricov['datediff'])
        #df_user_ricov['riammissione'] = df_user_ricov['datediff'].apply( lambda x : 10 <= int(x) < 30)
        df_prestazioni_all = df_prestazioni_all.append(df_user_ricov)#,ignore_index=True)

    #################### dal df dei risultati mi calcolo delle colonne di servizio per capire se l'esame è in/sotto/sopra soglia ########################################################

    df_risultati_lab['codice_esame']= df_risultati_lab.codice_esame.replace('B2GM1','B2GM').replace('B2GG1','B2GG')
    df_risultati_lab['risultato'] = df_risultati_lab.risultato.replace('>','').replace('<','')
    len(df_risultati_lab.codice_esame.unique())
    df_risultati_lab = df_risultati_lab.dropna()
    ex_max = []
    ex_min = []
    ex_ok = []
    ex_problem = []
    for i in range(df_risultati_lab.shape[0]):
        if bool(re.search('[a-zA-Z<>]', df_risultati_lab.iloc[i].risultato)):
            ex_max.append(0)
            ex_min.append(0)
            ex_ok.append(0)
            ex_problem.append(1)
        else:
            if (float(df_risultati_lab.iloc[i].risultato) >= float(df_risultati_lab.iloc[i].valore_massimo)):
                ex_max.append(1)
                ex_min.append(0)
                ex_ok.append(0)
                ex_problem.append(0)
            if (float(df_risultati_lab.iloc[i].risultato) <= float(df_risultati_lab.iloc[i].valore_minimo)):
                ex_max.append(0)
                ex_min.append(1)
                ex_ok.append(0)
                ex_problem.append(0)
            if (float(df_risultati_lab.iloc[i].valore_minimo)  < float(df_risultati_lab.iloc[i].risultato) < float(df_risultati_lab.iloc[i].valore_massimo)):
                ex_max.append(0)
                ex_min.append(0)
                ex_ok.append(1)
                ex_problem.append(0)


    df_risultati_lab.loc[:, 'ris_min'] = ex_min
    df_risultati_lab.loc[:, 'ris_ok'] = ex_ok
    df_risultati_lab.loc[:, 'ris_max'] = ex_max
    df_risultati_lab.loc[:, 'ris_problem'] = ex_problem

    ########################  CALCOLO RESONTO ESAME E AGGIUNGO ANAGRAFICA #####################################################################

    df_row= pd.DataFrame()
    df_final=pd.DataFrame()
    for paziente in list(df_risultati_lab.gdpr_id.unique()):
        df_init = pd.DataFrame({'gdpr_id':[paziente]})
        #cod_exames = list(df_risultati_lab.codice_esame.unique())   # TODO: INSERIRE A MANO LA LISTA DI TUTTI GLI ESAMI
        cod_exames = ['ALBU1', 'MICR', 'RAC3', 'TRIG', 'LDL', 'MICA', 'MICA24', 'GLU24', 
                      'GLUM', 'PALB', 'PG', 'B2GG', 'B2GM', 'CG3', 'CG', 'F76', 'CI', 'CGM', 'F232', 'CACL1']
        i=0
        df_row= pd.DataFrame()
        for cod_exam in cod_exames:     
            tot = df_risultati_lab[(df_risultati_lab['gdpr_id'] == int(paziente)) & 
                                   (df_risultati_lab['codice_esame']==cod_exam)].shape[0]
            if tot != 0:
                df_pz = df_risultati_lab[(df_risultati_lab['gdpr_id'] == int(paziente))]
                df_es =  df_pz[df_pz['codice_esame']==cod_exam] 
                df_prob=df_es[df_es['ris_problem'] !=1]
                if df_prob.shape[0] !=0:
                    head=float(df_prob.sort_values(by=['rich_date']).risultato.head(1))
                    tail= float(df_prob.sort_values(by=['rich_date']).risultato.tail(1))
                    trend=  (tail - head) /tot
                else:
                    tail = float(0)
                    head = float(0)
                    trend = float(-99)
            else:
                tail = float(0)
                head = float(0)
                trend = float(-99)
            
            ok_perc = round(df_risultati_lab.loc[(df_risultati_lab['gdpr_id'] == int(paziente)) & (df_risultati_lab['codice_esame']==str(cod_exam)), 'ris_ok'].sum()/tot, 2)
            min_perc = round(df_risultati_lab.loc[(df_risultati_lab['gdpr_id'] == int(paziente)) & (df_risultati_lab['codice_esame']==str(cod_exam)), 'ris_min'].sum()/tot, 2)
            max_perc = round(df_risultati_lab.loc[(df_risultati_lab['gdpr_id'] == int(paziente)) & (df_risultati_lab['codice_esame']==str(cod_exam)), 'ris_max'].sum()/tot, 2)
            problem_perc = round(df_risultati_lab.loc[(df_risultati_lab['gdpr_id'] == int(paziente)) & (df_risultati_lab['codice_esame']==str(cod_exam)), 'ris_problem'].sum()/tot,2)
            
            df_temp1 = pd.DataFrame({'gdpr_id':[paziente],
                                     cod_exam+'_tot':[tot],
                                     cod_exam+'_ok':[ok_perc],
                                     cod_exam+'_low':[min_perc],
                                     cod_exam+'_hight':[max_perc],
                                     cod_exam+'_problem':[problem_perc],
                                     cod_exam+'_trend':[trend]
            })
            if i != 0:
                df_row = pd.merge(df_row, df_temp1, on='gdpr_id')
            else:
                df_row = df_temp1

            i=i+1
        df_final = pd.concat([df_final, df_row], ignore_index = True)
    df_final.head(20)
    result = pd.merge(df_final, df_master, on='gdpr_id').drop(['esenzione', 'inizio_validita','fine_validita'], axis=1)  
    result=result.drop(result.columns[(result == 0).all()], axis=1) 
    result.rename(columns = {'eta_inizio_esenzione':'eta'}, inplace = True)
    result.fillna(0, inplace = True)
    result['decesso']=result.data_decesso.apply(lambda x: True if (x!=0) else False)

    ########################## AGGIUNGO STATISTICHE SUI RICOVERI ###########################################################################

    df_ricoveri_paz=pd.DataFrame()
    for paziente in list(df_risultati_lab.gdpr_id.unique()):
        totale = df_prestazioni_all[(df_prestazioni_all['gdpr_id'] == paziente)].shape[0]
        totale_fake = totale
        if totale ==0:
            totale_fake=1
        df_temp1 = pd.DataFrame({'gdpr_id':[paziente]
                ,'perc_PS':df_prestazioni_all[(df_prestazioni_all['gdpr_id'] == paziente) & (df_prestazioni_all['tipoprestazione'] == 'PS' )].shape[0]/totale_fake
                ,'perc_ambulatorio':df_prestazioni_all[(df_prestazioni_all['gdpr_id'] == paziente) & (df_prestazioni_all['tipoprestazione'] == 'ambulatoriale' )].shape[0]/totale_fake
                ,'perc_ricoverato':df_prestazioni_all[(df_prestazioni_all['gdpr_id'] == paziente) & (df_prestazioni_all['tipoprestazione'] == 'ricoverato' )].shape[0]/totale_fake
                ,'num_prestazioni_tot':totale
                #,'riammissione':df_prestazioni_all_label[(df_prestazioni_all['gdpr_id'] == paziente) & (df_prestazioni_all_label['riammissione'] == True )].shape[0]>0
        })
        df_ricoveri_paz= df_ricoveri_paz.append(df_temp1)
    feat_ricoveri= pd.merge(result, df_ricoveri_paz, on='gdpr_id')


    ############################ AGGIUNGO FEATURE SUI RICOVERI ##############################################################################################
    users =  feat_ricoveri.gdpr_id.unique()
    df_prestazioni_all = df_prestazioni_all.drop(df_prestazioni_all[df_prestazioni_all.data_erogazione.isna()].index)
    result_row= pd.DataFrame()
    result_recovery= pd.DataFrame()
    for user in users:
        df_user_ric= user_recovery_features(user, df_prestazioni_all)
        result_recovery= result_recovery.append(df_user_ric)

    result_final = pd.merge(feat_ricoveri,result_recovery, on='gdpr_id')    

    features_list = ['RAC3_tot', 'RAC3_ok', 'RAC3_low', 'RAC3_hight', 'RAC3_trend',
       'TRIG_tot', 'TRIG_ok', 'TRIG_low', 'TRIG_hight', 'TRIG_problem',
       'TRIG_trend', 'LDL_tot', 'LDL_ok', 'LDL_low', 'LDL_hight', 'LDL_trend',
       'MICA24_tot', 'MICA24_ok', 'MICA24_low', 'MICA24_hight',
       'MICA24_problem', 'MICA24_trend', 'GLUM_tot', 'GLUM_ok', 'GLUM_low',
       'GLUM_hight', 'GLUM_trend', 'PALB_tot', 'PALB_ok', 'PALB_low',
       'PALB_trend', 'B2GG_tot', 'B2GG_ok', 'B2GG_low', 'B2GG_hight',
       'B2GG_trend', 'B2GM_tot', 'B2GM_ok', 'B2GM_low', 'B2GM_hight',
       'B2GM_trend', 'CG3_tot', 'CG3_ok', 'CG3_low', 'CG3_hight', 'CG3_trend',
       'CG_tot', 'CG_ok', 'CG_hight', 'CG_trend', 'CI_tot', 'CI_ok', 'CI_low',
       'CI_hight', 'CI_trend', 'CGM_tot', 'CGM_ok', 'CGM_hight', 'CGM_trend',
       'GLU24_tot', 'GLU24_problem', 'CACL1_tot', 'CACL1_hight', 'CACL1_trend',
       'eta', 'perc_PS', 'perc_ambulatorio', 'perc_ricoverato',
       'num_prestazioni_tot', 'n_ricoveri', 'n_days_tot_rec', 'max_days_rec',
       'mean_days_rec', 'sesso_F', 'sesso_M', 'decesso_False', 'decesso_True']

    input_dataset = pd.DataFrame(columns = features_list)

    for column in features_list:
        try:
            input_dataset[column] = result_final[column]
        except:
            input_dataset[column] = -99

    if result_final.loc[0, 'sesso'] == 'M':
        input_dataset['sesso_M'] = 1
        input_dataset['sesso_F'] = 0
    else:
        input_dataset['sesso_M'] = 0
        input_dataset['sesso_F'] = 1

    if result_final.loc[0, 'decesso'] == 'False':
        input_dataset['decesso_False'] = 1
        input_dataset['decesso_True'] = 0
    else:
        input_dataset['decesso_False'] = 0
        input_dataset['decesso_True'] = 1

    raw_data = input_dataset.fillna(value = 0, inplace = False).values.tolist()

    return raw_data

    
    
