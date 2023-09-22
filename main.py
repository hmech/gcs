#Run notes: 
# command: python main.py --config default.yaml
# Make sure that default.yaml contains all required things
# Make sure bq group has been created and update line 269

import sys
from typing import List

from google.auth import impersonated_credentials, default as application_default_credentials
from google.auth.credentials import Credentials
from google.cloud import bigquery, billing, logging, resourcemanager, service_usage, storage
from google.cloud.bigquery.dataset import AccessEntry, DatasetReference
from google.cloud.bigquery.enums import EntityTypes
from google.iam.v1.policy_pb2 import Policy, Binding
from google.iam.v1.iam_policy_pb2 import SetIamPolicyRequest


from googleapiclient.discovery import build

from Arguments import Arguments, ArgumentParser

CUSTOMER_FOLDER = "customers"
CUSTOMER_FOLDER_ID = 152141514306
CUSTOMER_TRIALS_FOLDER_ID = 338987899866
BILLING_ACCOUNT_ID = "015E69-2DE2C4-D9B22D"


def create_quota(credentials: Credentials, parent: str, metric: str, unit: str, quota: int):
    quotas_service = build(serviceName="serviceusage", version="v1beta1", credentials=credentials)
    quotas_service.services().consumerQuotaMetrics().limits().consumerOverrides().create(
        parent=parent,
        body={
            "metric": metric,
            "unit": unit,
            "overrideValue": quota,
        },
        force=True,
    ).execute()


def patch_quota(credentials: Credentials, name: str, quota: str):
    quotas_service = build(serviceName="serviceusage", version="v1beta1", credentials=credentials)
    quotas_service.services().consumerQuotaMetrics().limits().consumerOverrides().patch(
        name=name,
        force=True,
        body={
            "overrideValue": quota
        }
    ).execute()


def set_big_query_quotas(
        credentials: Credentials,
        project_id: str,
        project_quota: int,
        user_quota: int
):
    projects_client = resourcemanager.ProjectsClient(credentials=credentials)
    project = projects_client.get_project(name=f"projects/{project_id}")
    project_number = project.name
    quotas_service = build(serviceName="serviceusage", version="v1beta1", credentials=credentials)
    all_bigquery_quotas = quotas_service \
        .services() \
        .consumerQuotaMetrics() \
        .list(parent=f"{project_number}/services/bigquery.googleapis.com") \
        .execute()
    usage_quota = None
    for quota in all_bigquery_quotas["metrics"]:
        if quota[
            "name"] == f'{project_number}/services/bigquery.googleapis.com/consumerQuotaMetrics/bigquery.googleapis.com%2Fquota%2Fquery%2Fusage':
            usage_quota = quota
            break

    project_quota_policy = {
        'unit': '1/d/{project}',
        'quota': project_quota
    }

    user_quota_policy = {
        'unit': '1/d/{project}/{user}',
        'quota': user_quota
    }

    for quota_limit in usage_quota["consumerQuotaLimits"]:
        if quota_limit[
            "name"] == f'{project_number}/services/bigquery.googleapis.com/consumerQuotaMetrics/bigquery.googleapis.com%2Fquota%2Fquery%2Fusage/limits/%2Fd%2Fproject':
            project_quota_policy['limit'] = quota_limit
        if quota_limit[
            "name"] == f'{project_number}/services/bigquery.googleapis.com/consumerQuotaMetrics/bigquery.googleapis.com%2Fquota%2Fquery%2Fusage/limits/%2Fd%2Fproject%2Fuser':
            user_quota_policy['limit'] = quota_limit

    for big_query_quota in [project_quota_policy, user_quota_policy]:
        if big_query_quota['limit']['quotaBuckets'][0].get('consumerOverride') is None:
            create_quota(
                credentials=credentials,
                parent=big_query_quota['limit']['name'],
                metric='bigquery.googleapis.com/quota/query/usage',
                unit=big_query_quota['unit'],
                quota=big_query_quota['quota']
            )
        else:
            patch_quota(
                credentials=credentials,
                name=big_query_quota['limit']['quotaBuckets'][0]['consumerOverride']['name'],
                quota=big_query_quota['quota']
            )


def enable_google_api(credentials: Credentials, project_id: str, api_name):
    print(f"Enabling API: {api_name}")

    client = service_usage.ServiceUsageClient(credentials=credentials)
    operation = client.enable_service({
        "name": f"projects/{project_id}/services/{api_name}"
    })
    operation.result()


def create_project(credentials: Credentials, project_id: str, group_name: str):
    print(f"Creating new project: {project_id}")

    client = resourcemanager.ProjectsClient(credentials=credentials)
    client.create_project({
        "project": {
            "parent": f"folders/{CUSTOMER_FOLDER_ID}",
            "display_name": project_id,
            "project_id": project_id
        }
    }).result()

    print("Linking Billing Account")
    enable_google_api(credentials=credentials, project_id=project_id, api_name="cloudbilling.googleapis.com")
    billing_client = billing.CloudBillingClient(credentials=credentials)
    billing_client.update_project_billing_info({
        "name": f"projects/{project_id}",
        "project_billing_info": {
            "billing_account_name": f"billingAccounts/{BILLING_ACCOUNT_ID}"
        }
    })

    print("Assigning Roles")
    project_policy: Policy = client.get_iam_policy({
        "resource": f"projects/{project_id}"
    })

    for role_name, user_names in {
        "roles/owner": ["user:tutela@tutelatech.com"],
        "organizations/48397401872/roles/ExternalCustomerRole": [f"group:{group_name}"]
    }.items():
        print(f"Assigning roles: {user_names} -> {str(role_name)}")
        binding = None
        for policy_binding in project_policy.bindings:
            if policy_binding.role == role_name:
                binding = policy_binding
                break
        if binding is not None:
            binding.members[:] = user_names
        else:
            project_policy.bindings.append(
                Binding(role=role_name, members=user_names)
            )

    client.set_iam_policy(
        SetIamPolicyRequest(
            resource=f"projects/{project_id}",
            policy=project_policy
        )
    )


def configure_bigquery(credentials: Credentials,
                       project_id: str,
                       group_name: str,
                       standard_datasets: List[str],
                       customer_datasets: List[str],
                       extra_datasets: List[str],
                       project_quota: int,
                       user_quota: int
                       ):
    print("Configuring Big Query")

    enable_google_api(credentials=credentials, project_id=project_id, api_name="bigquery.googleapis.com")
    enable_google_api(credentials=credentials, project_id=project_id, api_name="bigqueryreservation.googleapis.com")
    enable_google_api(credentials=credentials, project_id=project_id, api_name="logging.googleapis.com")

    big_query_client = bigquery.Client(credentials=credentials, project=project_id)

    for dataset_id in standard_datasets + customer_datasets + extra_datasets:
        print(f"Creating dataset: {dataset_id}")
        big_query_client.create_dataset(DatasetReference(project=project_id, dataset_id=dataset_id), exists_ok=True)

    # TODO remomve the servicve account that created the dataset from owners
    for dataset_id, role in [(dataset_id, "READER") for dataset_id in standard_datasets] + \
                            [(dataset_id, "WRITER") for dataset_id in customer_datasets]: 
        print(f"Granting customer {role} to dataset: {dataset_id}")

        dataset = big_query_client.get_dataset(DatasetReference(project=project_id, dataset_id=dataset_id))
        entries = list(dataset.access_entries)
        entries = [
            entry for entry in entries if entry.entity_id != credentials.service_account_email
        ]

        entries.append(AccessEntry(
            role=role,
            entity_type=EntityTypes.GROUP_BY_EMAIL,
            entity_id=group_name
        ))
        dataset.access_entries = entries
        big_query_client.update_dataset(dataset, ["access_entries"])

        #Adding a default expiration date for all standard/customer datasets excluding Region_Files
        if dataset != 'Region_Files':
            dataset.default_table_expiration_ms = 548 * 24 * 60 * 60 * 1000  #548 Days In milliseconds.
            #big_query_client.update_dataset(dataset, ["default_table_expiration_ms"]) 

    print("Setting Quotas")
    set_big_query_quotas(credentials=credentials, project_id=project_id, project_quota=project_quota,
                         user_quota=user_quota)


def configure_logging(credentials: Credentials, project_name: str):
    print(f"Creating Log Sink")

    logging_service = build(serviceName="logging", version="v2", credentials=credentials)
    # logging_service.sinks().get(sinkName=f"projects/{project_name}/sinks/BQ_Query_Usage")
    try:
        sink = logging_service.sinks().get(sinkName=f"projects/{project_name}/sinks/BQ_Query_Usage").execute()
    except Exception as e:
        sink = logging_service.sinks().create(
            parent=f"projects/{project_name}",
            uniqueWriterIdentity=True,
            body={
                "name": "BQ_Query_Usage",
                "bigqueryOptions": {
                    "usePartitionedTables": True
                },
                "description": "Standard Big Query Logging",
                "destination": f"bigquery.googleapis.com/projects/{project_name}/datasets/Logs",
                "filter": 'resource.type="bigquery_resource" protoPayload.serviceData.jobCompletedEvent.eventName="query_job_completed"'

            }
        ).execute()
    service_account = sink["writerIdentity"]
    big_query_client = bigquery.Client(credentials=credentials, project=project_name)
    dataset = big_query_client.get_dataset(DatasetReference(project=project_name, dataset_id="Logs"))
    entries = list(dataset.access_entries)
    entries = [
        entry for entry in entries if entry.entity_id != credentials.service_account_email
    ]

    entries.append(AccessEntry(
        role="roles/bigquery.dataEditor",
        entity_type=EntityTypes.IAM_MEMBER,
        entity_id=f"{service_account}"
    ))
    dataset.access_entries = entries
    big_query_client.update_dataset(dataset, ["access_entries"])

def configure_cloud_storage(credentials: Credentials, project_id: str):
    print(f"Creating bucket: {project_id}")

    client = storage.Client(credentials=credentials, project=project_id)
    client.create_bucket(bucket_or_name=project_id)


def main(argv):
    arguments = ArgumentParser().parse(argv)

    print(f"Creating project with configuration: {str(arguments)}")
    print(arguments.project_id)

    project_id = arguments.project_id
    #TODO get group name from args and remove customer name
    customer_name = arguments.customer_name
    group_name = 'tutela-external-onxsmartph@comlinkdata.com' #f"tutela-external-{customer_name.replace(' ', '').lower()}@comlinkdata.com"
    standard_datasets = arguments.standard_datasets
    customer_datasets = arguments.customer_datasets
    extra_datasets = arguments.extra_datasets
    project_quota_mb = arguments.project_quota
    user_quota_mb = arguments.user_quota

    source_credentials, project = application_default_credentials() #Creating invalid credentials
    credentials = impersonated_credentials.Credentials(
        source_credentials=source_credentials,
        target_principal='cs-create-customer-projects@tutela-auxiliary-team.iam.gserviceaccount.com',
        target_scopes=[]
    )
    create_project(
        credentials=credentials,
        project_id=project_id,
        group_name=group_name
    )

    configure_bigquery(
        credentials=credentials,
        project_id=project_id,
        group_name=group_name,
        standard_datasets=standard_datasets,
        customer_datasets=customer_datasets,
        extra_datasets=extra_datasets,
        project_quota=project_quota_mb,
        user_quota=user_quota_mb
    )

    configure_logging(
        credentials=credentials,
        project_name=project_id
    )

    configure_cloud_storage(
        credentials=credentials,
        project_id=project_id
    )

    #print("Test finished, Goodbye")
    print("Goodbye")

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
