#!/bin/bash

#create file names
max=17
files_list=("AA" "AB" "AC" "AD" "AE" "AF" "AG" "AI" "AL" "AM" "AN" "AO" "AP" "AQ" "AR" "AS" "AT" "AU" "AW" "AZ" "BA" "BB" "BD" "BE" "BF" "BG" "BH" "BI" "BJ" "BL" "BM" "BN" "BO" "BQ" "BR" "BS" "BT" "BU" "BW" "BY" "BZ" "CA" "CC" "CD" "CF" "CG" "CH" "CI" "CK" "CL" "CM" "CN" "CO" "CP" "CQ" "CR" "CS" "CU" "CV" "CW" "CY" "CZ" "DA" "DE" "DJ" "DK" "DL" "DM" "DO" "DU" "DV" "DZ" "EC" "EE" "EG" "EH" "ER" "ES" "ET" "EV" "FC" "FD" "FH" "FI" "FJ" "FL" "FM" "FO" "FR" "GA" "GB" "GD" "GE" "GF" "GG" "GH" "GI" "GJ" "GL" "GM" "GN" "GO" "GP" "GQ" "GR" "GT" "GU" "GW" "GY" "HA" "HC" "HD" "HE" "HK" "HN" "HR" "HT" "HU" "HZ" "IB" "ID" "id" "IE" "II" "IL" "IM" "IN" "IQ" "IR" "IS" "IT" "IZ" "JA" "JB" "JE" "JI" "JK" "JL" "JM" "JO" "JP" "JR" "JT" "KA" "KB" "KE" "KG" "KH" "KI" "KL" "KN" "KP" "KR" "KS" "KT" "KW" "KY" "KZ" "LA" "LB" "LC" "LD" "LI" "LK" "LN" "LR" "LS" "LT" "LU" "LV" "LY" "MA" "MC" "MD" "ME" "MF" "MG" "MH" "MK" "ML" "MM" "MN" "MO" "MQ" "MR" "MS" "MT" "MU" "MV" "MW" "MX" "MY" "MZ" "NA" "NB" "NC" "NE" "NG" "NH" "NI" "NL" "NO" "NP" "NS" "NT" "NW" "NY" "NZ" "OG" "OM" "ON" "PA" "PB" "PE" "PF" "PG" "PH" "PK" "PL" "PR" "PS" "PT" "PW" "PY" "QA" "QC" "QQ" "QU" "RA" "RE" "RI" "RJ" "RK" "RN" "RO" "RP" "RS" "RU" "RV" "RW" "SA" "SC" "SD" "SE" "SH" "SI" "SJ" "SK" "SL" "SM" "SN" "SO" "SR" "SS" "ST" "SU" "SV" "SX" "SY" "SZ" "TA" "TC" "TD" "TG" "Þ­" "TH" "TJ" "TL" "TM" "TN" "TO" "TR" "TT" "TU" "TV" "TW" "TZ" "UA" "UG" "UK" "UQ" "US" "UY" "UZ" "VA" "VB" "VC" "VE" "VG" "VI" "VN" "VU" "WA" "WH" "WN" "WS" "WU" "WZ" "XK" "XO" "XP" "XX" "YE" "YF" "YI" "YM" "YO" "ZA" "ZE" "ZG" "ZH" "ZM" "ZW" "ZZ" )
for j in "${files_list[@]}"
do
x=1
echo "$j"
	for i in `seq 1 $max`
	do
	fileone=$j
    	file_two=_discont_stickers_
    	new_file_name=$fileone$file_two$x
        echo "$new_file_name"
     	part_one='https://fiji.bbmessaging.com/api/v1//sticker_packs?&platform=android&country='
	part_two='&license_type=discontinued&start='
    	part_three='&limit=2100'
    	api=$part_one$j$part_two$x$part_three
    	echo "$api" 
    	curl -s "$api" | jq '.stickerpacks[] |  [.id,.licenseType] |@csv ' -M >  all_stickers/"$new_file_name"
    	x=$((x+120))
	done
done
