<?php

namespace App\Http\Controllers;
use App\Models\FeToken;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\DB;

class UserController extends Controller 
{
    public function actionLogin(Request $request)
    {



        $FeToken = FeToken::get();
        $results = DB::connection('pgsql')->select('SELECT * FROM ventas');
        dd($results);
        dd($FeToken);


        
    }
}
