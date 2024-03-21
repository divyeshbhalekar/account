import logging
import os
import requests

FORMAT = '%(asctime)s\t| %(levelname)s | line %(lineno)d, %(name)s, func: %(funcName)s()\n%(message)s\n'
logging.basicConfig(format=FORMAT)

logger = logging.getLogger("scan_option")
logger.setLevel(level=logging.INFO)

POLICY_ENGINE_URL = os.getenv("POLICY_ENGINE_URL")
SCAN_SETTINGS_URL = os.getenv("SCAN_SETTINGS_URL")


def check_scan_setting(company_id):
    logger.info(f'In check_scan_setting')
    logger.debug(f'Api call to policy engine for check scan settings for company_id: {company_id}')
    url = POLICY_ENGINE_URL + '/scan_options/scan_settings_and_schedular'
    header={"Content-Type": "application/json; charset=utf-8"}
    scan_setting_payload = {
        "company_id": company_id,
        "asset_type": ["dlp"]
    }
    res = requests.post(url=url, headers=header, json=scan_setting_payload)
    if res.json():
        logger.info(f'Response received from scan setting is {res.json()}')
        results = res.json()[0]
        if results['dlp']['status'] and (
                results['dlp']['risks']['pii'] or results['dlp']['risks']['pci'] or results['dlp']['risks']['hipaa'] or
                results['dlp']['risks']['confidential']):
            dlp_scan_setting = True
        else:
            dlp_scan_setting = False

        malware_scan_setting = results['dlp']['malware']['cloud_storage']
        if dlp_scan_setting & malware_scan_setting:
            return {"message": "Scan Settings Available"}
        else:
            logger.info(f'Scan Setting not available for the companyid {company_id}')
            return None 
    else:
        logger.info(f'Scan Setting not available for the companyid {company_id}')
        return None 


def send_scan_data(request):
    company_id=request.company_id
    status=check_scan_setting(company_id)
    if status==None:
        url = POLICY_ENGINE_URL + '/scan_options/scan_settings'
        header={"Content-Type": "application/json; charset=utf-8"}
        parameter = {'id': company_id}
        scan_setting_payload = {
                        "clouds": {
                            "cloud_provider": [
                                "azure",
                                "aws",
                                "gcp"
                            ],
                            "cloud_regions": [],
                            "profile_id": [
                                "all"
                            ]
                        },
                        "kubernetes": {
                            "cluster_names": [
                                "all"
                            ]
                        },
                        "dlp": {
                            "status": False,
                            "risks": {
                                "pii": False,
                                "pci": False,
                                "hipaa": False,
                                "confidential": False
                            },
                            "malware": {
                                "cloud_storage": False,
                                "attached_volumes": False
                            }
                        },
                        "mscan": {
                            "status": True
                        },
                        "serverless": {
                            "enable":  True
                        },
                        "compliances": {
                            "status":  True,
                            "aws": {},
                            "azure": {},
                            "gcp": {}
                        },
                        "risks": {
                            "status": False,
                            "cloud_risks": {
                                "open_ports": False,
                                "cloud_misconfigurations": False,
                                "unmanaged_databases": False,
                                "cloud_vulnerabilities": False
                            },
                            "kubernetes_risks": {
                                "kubernetes_misconfigurations": False,
                                "microsegmentation": False,
                                "kubernetes_vulnerabilities": False
                            }
                        },
                        "data_classification": {
                            "status": False,
                            "data_loss_prevention": {
                                "kubernetes_persistent_volumes": False,
                                "kubernetes_east_west_traffic": False
                            }
                        },
                        "scan_model": {
                            "compute_in_microsec_cloud_account": False,
                            "compute_in_my_cloud_account": False
                        }
                    }
        res = requests.post(url=url, headers=header, json=scan_setting_payload,params=parameter)
        logger.info(f'scan settings created for companyid {company_id} with response {res.json()}')
        return {"message": "Scan Settings Created"}
    else:
        return {"message": "Scan Settings Available"}

