<?php

use App\Http\Controllers\TransferirDataController;
use Illuminate\Support\Facades\Route;

Route::any('/', [TransferirDataController::class, 'actionTransferirVentasAtendidas']);
Route::any('/ventas', [TransferirDataController::class, 'actionTransferirVentas']);
Route::any('/acopio', [TransferirDataController::class, 'actionTransferirAcopio']);
Route::any('/ventassincosto', [TransferirDataController::class, 'actionTransferirVentasSinCosto']);

Route::any('/ventasconcosto', [TransferirDataController::class, 'actionTransferirVentasConCosto']);
