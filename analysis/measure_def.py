###################################################
# This script creates year counts/rates of atorvastatin prescribing
# in patients aged 40-84 with a QRISK score of 5-10%, 
# and with no history of cardiovascular events.
#
# Author: Colm Andrews
#   Bennett Institute for Applied Data Science
#   University of Oxford, 2025
###################################################
from ehrql import (
    INTERVAL, 
    create_measures, 
    months,
    years, 
    case, 
    when, 
    codelist_from_csv
)

from ehrql.tables.tpp import (
    patients, 
    practice_registrations, 
    addresses, 
    clinical_events,
    medications
)

import codelists

###atorvastatin codelist
atorvastatin_20_codes = codelist_from_csv(
  "codelists/user-candrews-atorvastatin-20-drug-codes.csv",
  column="code"
)

qrisk_scores = codelist_from_csv(
  "codelists/user-candrews-qrisk-observable-entities.csv",
  column="code"
)


ethnicity5 = codelist_from_csv(
  "codelists/opensafely-ethnicity-snomed-0removed.csv",
  column="code",
  category_column="Label_6", # it's 6 because there is an additional "6 - Not stated" but this is not represented in SNOMED, instead corresponding to no ethnicity code
)

chd_cod = codelist_from_csv(
  "codelists/nhsd-primary-care-domain-refsets-chd_cod.csv",
  column="code"
)

estrk_cod = codelist_from_csv(
  "codelists/nhsd-primary-care-domain-refsets-strk_cod.csv",
  column="code"
)
 

tia_cod = codelist_from_csv(
  "codelists/nhsd-primary-care-domain-refsets-tia_cod.csv",
  column="code"
)

pad_cod = codelist_from_csv(
  "codelists/nhsd-primary-care-domain-refsets-pad_cod.csv",
  column="code"
)

dmtype1_cod = codelist_from_csv(
  "codelists/nhsd-primary-care-domain-refsets-dmtype1_cod.csv",
  column="code"
)
 
ckd12_cod = codelist_from_csv(
  "codelists/nhsd-primary-care-domain-refsets-ckd1and2_cod.csv",
  column="code"
)
 
ckd_cod = codelist_from_csv(
  "codelists/nhsd-primary-care-domain-refsets-ckd_cod.csv",
  column="code"
)
 
ckdres_cod = codelist_from_csv(
  "codelists/nhsd-primary-care-domain-refsets-ckdres_cod.csv",
  column="code"
)

classfh_cod = codelist_from_csv(
  "codelists/nhsd-primary-care-domain-refsets-classfh_cod.csv",
  column="code"
)

famhypgen_cod = codelist_from_csv(
  "codelists/nhsd-primary-care-domain-refsets-famhypgen_cod.csv",
  column="code"
)

possfh_cod = codelist_from_csv(
  "codelists/nhsd-primary-care-domain-refsets-possfh_cod.csv",
  column="code"
)

famhypref_cod = codelist_from_csv(
  "codelists/nhsd-primary-care-domain-refsets-famhypref_cod.csv",
  column="code"
)


##########
#Numerator: patients with a prescription of atorvastatin 20mg
atorvastatin_20 = medications.where(
                medications.dmd_code.is_in(atorvastatin_20_codes)
                & medications.date.is_during(INTERVAL)).exists_for_patient()


#Denominator: inclusion criteria
## Include people alive
is_alive = patients.is_alive_on(INTERVAL.start_date)

qrisk_5_10 = (
    clinical_events.where(
        clinical_events.snomedct_code.is_in(qrisk_scores)
        # values between 5 and 10
        & (clinical_events.numeric_value > 5)
        & (clinical_events.numeric_value < 10)
        & (clinical_events.date.is_on_or_between(INTERVAL.start_date-months(3),INTERVAL.start_date))
    ).sort_by(clinical_events.date)
    .last_for_patient()
)

## Include people registered with a TPP practice for at least 3 months
is_registered =  (
    practice_registrations.where(
        practice_registrations.start_date.is_on_or_before(INTERVAL.start_date - months(3))
    )
    .except_where(
        practice_registrations.end_date.is_on_or_before(INTERVAL.start_date)
    )
).exists_for_patient()

## Exclude people over 85 or under 40
has_possible_age= (patients.age_on(INTERVAL.start_date) < 85) & (patients.age_on(INTERVAL.start_date) >= 40) 

## Exclude people with non-male or female sex due to disclosure risk
non_disclosive_sex= (patients.sex == "male") | (patients.sex == "female")

### Exclude practices with fewer than 750 patients
## TODO, downstream? create measure with registered patients as denominator?

### Exclude practices with no QRISK coding
## TODO, downstream? create measure with registered patients with qrisk code as denominator?

### Exclude practices which opened or closed during the study period 
## downstream

## Prior Coronary heart disease
# CHD_REG Register of patients with a coronary heart disease (CHD) diagnosis.	
# If CHD_DAT ≠ Null Select

chd_reg = (
    clinical_events.where(clinical_events.snomedct_code.is_in(chd_cod)
    ).where(
        clinical_events.date.is_on_or_before(INTERVAL.start_date)
    ).exists_for_patient())

## Prior stroke
# Stroke/TIA register: Register of patients with a Stroke or TIA diagnosis.
# If ESTRK_DAT ≠ Null OR If TIA_DAT ≠ Null select

estrk =(
    clinical_events.where(clinical_events.snomedct_code.is_in(estrk_cod)
    ).where(
        clinical_events.date.is_on_or_before(INTERVAL.start_date)
    ).exists_for_patient() )


tia =(
    clinical_events.where(clinical_events.snomedct_code.is_in(tia_cod)
    ).where(
        clinical_events.date.is_on_or_before(INTERVAL.start_date)
    ).exists_for_patient() )

stroke_reg = (estrk | tia)

# peripheral arterial disease 
# If PAD_DAT ≠ Null
pad_reg = (
    clinical_events.where(clinical_events.snomedct_code.is_in(pad_cod)
    ).where(
        clinical_events.date.is_on_or_before(INTERVAL.start_date)
    ).exists_for_patient() )

# type 1 diabetes
dmtype1_reg =(
    clinical_events.where(clinical_events.snomedct_code.is_in(dmtype1_cod)
    ).where(
        clinical_events.date.is_on_or_before(INTERVAL.start_date)
    ).exists_for_patient() )

# ckd
# # Select patients passed to this rule who meet all of the criteria below:
# Patient has a chronic kidney disease (CKD) 3-5 diagnosis.
# CKD 3-5 diagnosis has not been resolved.
# CKD 3-5 diagnosis has not been superceded by a CKD 1-2 diagnosis.

ckd =(
    clinical_events.where(clinical_events.snomedct_code.is_in(ckd_cod)
    ).where(
        clinical_events.date.is_on_or_before(INTERVAL.start_date)
    ).exists_for_patient() )

ckd_dat = (
    clinical_events.where(clinical_events.snomedct_code.is_in(ckd_cod)
    ).where(
        clinical_events.date.is_on_or_before(INTERVAL.start_date)
    ).date.maximum_for_patient())

ckd12_dat =(
    clinical_events.where(clinical_events.snomedct_code.is_in(ckd12_cod)
    ).where(
        clinical_events.date.is_on_or_before(INTERVAL.start_date)
    ).date.maximum_for_patient())

ckdres_dat =(
    clinical_events.where(clinical_events.snomedct_code.is_in(ckdres_cod)
    ).where(
        clinical_events.date.is_on_or_before(INTERVAL.start_date)
    ).date.maximum_for_patient()) 

ckd_reg = (ckd & (ckd_dat>ckd12_dat) & (ckd_dat>ckdres_dat))

# familial hypercholesterolaemia
famhypgen = (
    clinical_events.where(clinical_events.snomedct_code.is_in(famhypgen_cod)
    ).where(
        clinical_events.date.is_on_or_before(INTERVAL.start_date)
    ).exists_for_patient() )

classfh = (
    clinical_events.where(clinical_events.snomedct_code.is_in(classfh_cod)
    ).where(
        clinical_events.date.is_on_or_before(INTERVAL.start_date)
    ).exists_for_patient() )

possfh = (
    clinical_events.where(clinical_events.snomedct_code.is_in(possfh_cod)
    ).where(
        clinical_events.date.is_on_or_before(INTERVAL.start_date)
    ).exists_for_patient() )

famhypref = (
    clinical_events.where(clinical_events.snomedct_code.is_in(famhypref_cod)
    ).where(
        clinical_events.date.is_on_or_before(INTERVAL.start_date)
    ).exists_for_patient() )

famhyp_reg = (famhypgen | classfh | possfh | famhypref)

# # define denominator
primary_denominator = ( is_registered
                        & ~chd_reg 
                        & ~stroke_reg 
                        & ~pad_reg 
                        & ~dmtype1_reg 
                        & ~ckd_reg 
                        & ~famhyp_reg  )


# #Specify intervals
intervals = months(2).starting_on("2018-01-01")


#Subgroups
## Age 
age = patients.age_on(INTERVAL.start_date)
age_band = case(
    when((age < 45)).then("0-44"),
    when((age >= 45) & (age < 65)).then("45-64"),
    when((age >= 65) & (age < 75)).then("65-74"),
    when((age >= 75) & (age < 85)).then("75-84"),
    when(age >= 85).then("85+"),
    )

# Practice
practice = practice_registrations.for_patient_on(INTERVAL.start_date).practice_pseudo_id 

## Practice region
region = practice_registrations.for_patient_on(INTERVAL.start_date).practice_nuts1_region_name
## Rurality
rural_urban = addresses.for_patient_on(INTERVAL.start_date).rural_urban_classification

#IMD
imd = addresses.for_patient_on(INTERVAL.start_date).imd_rounded

IMD_q10 = case(
        when((imd >= 0) & (imd < int(32844 * 1 / 10))).then("1 (most deprived)"),
        when(imd < int(32844 * 2 / 10)).then("2"),
        when(imd < int(32844 * 3 / 10)).then("3"),
        when(imd < int(32844 * 4 / 10)).then("4"),
        when(imd < int(32844 * 5 / 10)).then("5"),
        when(imd < int(32844 * 6 / 10)).then("6"),
        when(imd < int(32844 * 7 / 10)).then("7"),
        when(imd < int(32844 * 8 / 10)).then("8"),
        when(imd < int(32844 * 9 / 10)).then("9"),
        when(imd >= int(32844 * 9 / 10)).then("10 (least deprived)"),
        otherwise="unknown"
)


#Ethnicity
# Ethnicity

ethnicity = clinical_events.where(
        clinical_events.snomedct_code.is_in(ethnicity5)
    ).sort_by(
        clinical_events.date
    ).last_for_patient().snomedct_code.to_category(ethnicity5)

# Sex
sex = patients.sex

# # Create meassures
measures = create_measures()

measures.configure_dummy_data(population_size=1000)

measures.define_measure(
    "primary_atorvastatin_20",
    numerator= atorvastatin_20,
    denominator= primary_denominator,
    intervals=intervals,
    group_by={
        "practice": practice,
    }
)

# ## Age band
# measures.define_measure(
#     "primary_atorvastatin_20_age_band",
#     numerator= atorvastatin_20,
#     denominator= primary_denominator,
#     intervals=intervals,
#     group_by={
#         "age_band": age_band,
#         "practice": practice,
#     },
# )
# 
# Sex
# measures.define_measure(
#     "primary_atorvastatin_20_sex",
#     numerator= atorvastatin_20,
#     denominator= primary_denominator,
#     intervals=intervals,
#     group_by={
#         "sex": sex,
#         "practice": practice,
#     },
# )


# ## Practice region
# measures.define_measure(
#     "primary_atorvastatin_20_region",
#     numerator= atorvastatin_20,
#     denominator= primary_denominator,
#     intervals=intervals,
#     group_by={
#         "region": region,
#     },
# )
# ## Rurality 
# measures.define_measure(
#     "primary_atorvastatin_20_rural_urban",
#     numerator= atorvastatin_20,
#     denominator= primary_denominator,
#     intervals=intervals,
#     group_by={
#         "rural_urban": rural_urban,
#     },
# )

# ## IMD_q10
# measures.define_measure(
#     "primary_atorvastatin_20_IMD_q10",
#     numerator= atorvastatin_20,
#     denominator= primary_denominator,
#     intervals=intervals,
#     group_by={
#         "IMD_q10": IMD_q10,
#         "practice": practice,
#     },
# )
# ## Ethnicity
# measures.define_measure(
#     "primary_atorvastatin_20_ethnicity",
#     numerator= atorvastatin_20,
#     denominator= primary_denominator,
#     intervals=intervals,
#     group_by={
#         "ethnicity": ethnicity,
#         "practice": practice,
#     },
# )


