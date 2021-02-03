<?php
@ini_set('memory_limit',-1);

require '../vendor/autoload.php';

$timeZone = new DateTimeZone('Europe/Rome');


// host quadrature
// -------------------------------------------------------------------------------
$sourceHost = '10.11.14.128';
$sourceUser = 'root';
$sourcePassword = 'mela';

// host consolidati
// -------------------------------------------------------------------------------
$destinationHost = '10.11.14.177';
$destinationUser = 'root';
$destinationPassword = 'mela';

$data = new DateTime('2021-02-03', $timeZone);

try {
    // dichiarazione db
    // -------------------------------------------------------------------------------
    $sourceDb = new PDO("mysql:host=$sourceHost", $sourceUser, $sourcePassword, [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    $destinationDb = new PDO("mysql:host=$destinationHost", $destinationUser, $destinationPassword, [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);

    // carico la corrispondenza barcode => [codice articolo, reparto]
    // -------------------------------------------------------------------------------
    $stmt = "   select b.`BAR13-BAR2` barcode, b.`CODCIN-BAR2` codice, a.`IDSOTTOREPARTO` reparto 
                from archivi.barartx2 as b join dimensioni.articolo as a on b.`CODCIN-BAR2`=a.`CODICE_ARTICOLO`";

    $h_query = $sourceDb->prepare( $stmt );
    $h_query->execute();
    $result = $h_query->fetchAll( PDO::FETCH_ASSOC );
    $articles = [];
    foreach ($result as $article) {
        $articles[$article['barcode']] = ['codice' => $article['codice'], 'reparto' => $article['reparto']];
    }
    unset($result);

    // elenco dei sottoreparti validi
    // -------------------------------------------------------------------------------
    $stmt = "select idsottoreparto articleDepartment, desc_sottoreparto articleDepartmentDescription from mtx.sottoreparto";
    $h_query = $destinationDb->prepare( $stmt );
    $h_query->execute();
    $result = $h_query->fetchAll( PDO::FETCH_ASSOC );
    $validArticleDepartments = [];
    foreach ($result as $articleDepartment) {
        $validArticleDepartments[$articleDepartment['articleDepartment']] = $articleDepartment['articleDepartmentDescription'];
    }
    unset($result);

    // creazione base consolidati
    // -------------------------------------------------------------------------------
    $stmt = "select 
                store, 
                ddate, 
                reg, 
                trans, 
                userno `department`, 
                barcode,
                count(*) `rowCount`, 
                sum(quantita) `quantity`, 
                sum(totalamount) `totalamount`, 
                sum(totaltaxableamount) `totaltaxableamount` 
                from mtx.idc 
            where ddate = :data and binary recordtype = 'S' and recordcode1 = 1 
            group by 1,2,3,4,5,6";
    $h_query = $sourceDb->prepare($stmt);
    $h_query->execute([':data' => $data->format('Y-m-d')]);
    $result = $h_query->fetchAll(PDO::FETCH_ASSOC);

    $sourceDb = null;

    foreach ($result as $key => $row) {
        $result[$key]['weight'] = 0.0;

        $barcodeToSearch = $row['barcode'];
        if (preg_match('/^(\d{7})00000\d$/', $row['barcode'], $matches)) {
            $barcodeToSearch = $matches[1];
            $result[$key]['weight'] = $row['quantity'];
            if ($row['quantity'] > 0) { $result[$key]['quantity'] = 1; } else { $result[$key]['quantity'] = -1; };
        }

        $result[$key]['articlecode'] = (key_exists($barcodeToSearch, $articles)) ? $articles[$barcodeToSearch]['codice'] : '';
        $result[$key]['articledepartment'] = (key_exists($barcodeToSearch, $articles)) ? $articles[$barcodeToSearch]['reparto'] : '';
        if ($result[$key]['articledepartment'] == '') {
            if ($row['department'] == 1) {
                $result[$key]['articledepartment'] = '0100';
            } elseif ($row['department'] == 2) {
                $result[$key]['articledepartment'] = '0200';
            } elseif ($row['department'] == 3) {
                $result[$key]['articledepartment'] = '0300';
            } elseif ($row['department'] == 4) {
                $result[$key]['articledepartment'] = '0400';
            } elseif ($row['department'] == 5) {
                $result[$key]['articledepartment'] = '0405';
            } elseif ($row['department'] == 6) {
                $result[$key]['articledepartment'] = '0405';
            } elseif ($row['department'] == 7) {
                $result[$key]['articledepartment'] = '0407';
            } elseif ($row['department'] == 8) {
                $result[$key]['articledepartment'] = '0280';
            } elseif ($row['department'] == 9) {
                $result[$key]['articledepartment'] = '0280';
            } elseif($row['department'] > 9 && $row['department'] < 91) {
                $result[$key]['articledepartment'] = '0100';
            } elseif ($row['department'] == 91) {
                $result[$key]['articledepartment'] = '0901';
            } elseif($row['department'] > 91 && $row['department'] < 100) {
                $result[$key]['articledepartment'] = '0100';
            } else {
                $result[$key]['articledepartment'] = str_pad($row['department'], 4, "0", STR_PAD_LEFT);
            }
        }

        if (! key_exists($result[$key]['articledepartment'], $validArticleDepartments)) {
            echo "reparto " . $result[$key]['articledepartment'] . " inesistente!\n";
            $result[$key]['articledepartment'] = '0100';
        }

        if ($row['quantity'] < 0 && $row['totaltaxableamount'] > 0) {
            $result[$key]['totaltaxableamount'] = $result[$key]['totaltaxableamount'] * -1;
        }
    }

    $stmt = "insert into mtx.sales 
                (store,ddate,reg,trans,department,barcode,articledepartment,articlecode,weight,rowCount,quantity,totalamount,totaltaxableamount,fidelityCard)
             values 
                (:store,:ddate,:reg,:trans,:department,:barcode,:articledepartment,:articlecode,:weight,:rowCount,:quantity,:totalamount,:totaltaxableamount,:fidelityCard)";
    $h_query = $destinationDb->prepare($stmt);
    foreach ($result as $row) {
        $h_query->execute([
            ':store' => $row['store'],
            ':ddate' => $row['ddate'],
            ':reg' => $row['reg'],
            ':trans' => $row['trans'],
            ':department' => $row['department'],
            ':barcode' => $row['barcode'],
            ':articledepartment' => $row['articledepartment'],
            ':articlecode' => $row['articlecode'],
            ':weight' => $row['weight'],
            ':rowCount' => $row['rowCount'],
            ':quantity' => $row['quantity'],
            ':totalamount' => $row['totalamount'],
            ':totaltaxableamount' => $row['totaltaxableamount'],
            ':fidelityCard' => ''
        ]);
    }

    $stmt = "   insert into mtx.customers
                select s.store, s.ddate, count(distinct s.`reg`, s.`trans`) customerCount 
                from mtx.sales as s 
                where s.ddate = :ddate group by 1,2";
    $h_query = $destinationDb->prepare($stmt);
    $h_query->execute([':ddate' => $data->format('Y-m-d')]);

    $stmt = "   insert into mtx.penetration
                select s.store, s.ddate, t.`nuovoReparto` department, count(distinct s.`reg`, s.`trans`) customerCount 
                from mtx.sales as s left join mtx.sottoreparto as t on s.`articledepartment`=t.`idsottoreparto` 
                where s.ddate = :ddate group by 1,2,3;";
    $h_query = $destinationDb->prepare($stmt);
    $h_query->execute([':ddate' => $data->format('Y-m-d')]);

    echo "caricamento del giorno " . $data->format('Y-m-d') . " terminato\n";

} catch (PDOException $e) {
	echo "Errore: " . $e->getMessage();
	die();
}