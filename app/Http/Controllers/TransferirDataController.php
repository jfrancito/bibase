<?php

namespace App\Http\Controllers;
use App\Models\FeToken;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\DB;

use App\Traits\TranferirDataTraits;


class TransferirDataController extends Controller 
{

    use TranferirDataTraits;
    public function actionTransferirVentasAtendidas(Request $request)
    {
        $this->tdventasatendidas();        
    }


    //TRANSFERIR DATA
    public function actionTransferirVentas(Request $request)
    {
        $this->tdventas();        
    }

    public function actionTransferirAcopio(Request $request)
    {
        $this->tdacopio();        
    }

    public function actionTransferirVentasSinCosto(Request $request)
    {
        $this->tdventassincostos();        
    }


    public function actionTransferirVentasConCosto(Request $request)
    {
        $this->tdventasconcostos();        
    }

}
