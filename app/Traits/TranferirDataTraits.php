<?php

namespace App\Traits;

use App\Models\FeToken;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\DB;
use Carbon\Carbon;

trait TranferirDataTraits
{


    private function tdventasconcostos() {

        set_time_limit(0);

        /*
        |--------------------------------------------------------------------------
        | 1. Limpiar tabla destino (rápido)
        |--------------------------------------------------------------------------
        */
        DB::connection('pgsqlwin')->statement('TRUNCATE TABLE venta_arroz_costeada_raw RESTART IDENTITY');


        /*
        |--------------------------------------------------------------------------
        | 2. Procesar en chunks con orden estable
        |--------------------------------------------------------------------------
        */
        $totalProcesados = 0;

        DB::connection('sqlsrv_r')
            ->table('dpVentaSalidas2025')
            //->where('Orden','=','IICHVR0000055490')
            ->orderBy('FECHA')
            ->orderBy('ORDEN') // Campo estable (ajusta si tienes un ID único)
            ->chunk(1000, function ($ventas) use (&$totalProcesados) {
                if ($ventas->isEmpty()) {
                    return;
                }
                /*
                |--------------------------------------------------------------------------
                | 1. Obtener empresas del chunk
                |--------------------------------------------------------------------------
                */
                $empresas = $ventas->pluck('Empresa')->unique()->toArray();
                $empresasRelacion = DB::connection('sqlsrv_actual')
                    ->table('STD.EMPRESA')
                    ->whereIn('NOM_EMPR', $empresas)
                    ->pluck('COD_EMPR', 'NOM_EMPR')
                    ->toArray();

                /*
                |--------------------------------------------------------------------------
                | 2. Obtener centro del chunk
                |--------------------------------------------------------------------------
                */
                $centros = $ventas->pluck('Centro')->unique()->toArray();
                $centrosRelacion = DB::connection('sqlsrv_actual')
                    ->table('ALM.CENTRO')
                    ->where('COD_ESTADO','=','1')
                    ->whereIn('NOM_CENTRO', $centros)
                    ->pluck('COD_CENTRO', 'NOM_CENTRO')
                    ->toArray();

                /*
                |--------------------------------------------------------------------------
                | 3. Obtener clientes del chunk
                |--------------------------------------------------------------------------
                */
                $clientes = $ventas->pluck('Cliente')->unique()->toArray();
                $clientesRelacion = DB::connection('sqlsrv_actual')
                    ->table('STD.EMPRESA')
                    ->whereIn('NOM_EMPR', $clientes)
                    ->pluck('COD_EMPR', 'NOM_EMPR')
                    ->toArray();

                /*
                |--------------------------------------------------------------------------
                | 4. Obtener tipo clientes del chunk
                |--------------------------------------------------------------------------
                */
                $tclientes = $ventas->pluck('Cliente')->unique()->toArray();
                $tclientesRelacion = DB::connection('sqlsrv_actual')
                    ->table('STD.EMPRESA')
                    ->whereIn('NOM_EMPR', $clientes)
                    ->pluck('IND_RELACIONADO', 'NOM_EMPR')
                    ->toArray();
                /*
                |--------------------------------------------------------------------------
                | 5. Obtener productos del chunk
                |--------------------------------------------------------------------------
                */
                $codigosProducto = $ventas->pluck('NombreProducto')->unique()->toArray();
                $productosInfo = DB::connection('sqlsrv_actual')
                    ->table('ALM.PRODUCTO as P')
                    ->join('CMP.CATEGORIA as C', 'P.COD_CATEGORIA_TIPO_PRODUCTO', '=', 'C.COD_CATEGORIA')
                    ->whereIn('P.NOM_PRODUCTO', $codigosProducto)
                    ->select(
                        'P.NOM_PRODUCTO',
                        'P.COD_PRODUCTO',
                        'P.CAN_PESO_MATERIAL',
                        'C.NOM_CATEGORIA as tipo_producto',
                        'P.IND_MATERIAL_SERVICIO'
                    )
                    ->get()
                    ->keyBy('NOM_PRODUCTO')
                    ->toArray();

                /*
                |--------------------------------------------------------------------------
                | 5. Preparar inserción
                |--------------------------------------------------------------------------
                */
                $insertData = [];



                foreach ($ventas as $venta) {

                    // DESDE ACA SE EMPIEZA EL LLENADO //
                    // EMPRESA
                    $empresa = $empresasRelacion[$venta->Empresa] ?? null;
                    $empresa_id = $empresa ?? '';

                    //'INDUAMERICA INTERNACIONAL S.A.C.'
                    // CENTRO
                    $centro = $centrosRelacion[$venta->Centro] ?? null;
                    $centro_id = $centro ?? '';

                    $fecha = Carbon::parse($venta->Fecha);

                    // CLIENTE
                    $cliente = $clientesRelacion[$venta->Cliente] ?? null;
                    $cliente_id = $cliente ?? 0;

                    // TIPO CLIENTE
                    $indRelacionado = $tclientesRelacion[$venta->Cliente] ?? 0;
                    $tipoCliente = ($indRelacionado == 0) ? 'TERCERO' : 'RELACIONADA';

                    // PRODUCTO
                    $producto = $productosInfo[$venta->NombreProducto] ?? null;
                    $producto_id = $producto->COD_PRODUCTO ?? '';

                    $insertData[] = [

                        // NO enviamos id_venta → PostgreSQL lo genera
                        'empresa_codigo' => $empresa_id,
                        'empresa_nombre' => $venta->Empresa,
                        'centro_codigo' => $centro_id,
                        'centro_nombre' => $venta->Centro,
                        'fecha_venta' => date('Ymd', strtotime($venta->Fecha)),
                        'anio' => $fecha->year,
                        'mes' => $fecha->month,
                        'trimestre' => $fecha->quarter,
                        'numero_orden' => $venta->Orden,
                        'estado_venta' => $venta->Estado,

                        'cliente_codigo' => $cliente_id,
                        'cliente_nombre' => $venta->Cliente,
                        'tipo_cliente' => $tipoCliente,
                        'canal_venta' => $venta->Canal,
                        'subcanal_venta' => $venta->SubCanal,
                        'area_venta' => $venta->TipoVenta,
                        'jefe_venta' => $venta->JefeVenta,

                        'producto_codigo' => $producto_id,
                        'producto_nombre' => $venta->NombreProducto,

                        'familia_producto' => $venta->Familia,
                        'subfamilia_producto' => $venta->SubFamilia,
                        'unidad_medida' => $venta->UnidadMedida,                        

                        'cantidad_unidades' => $venta->CantidadProducto,
                        'peso_kilogramos' => $venta->Kg,
                        'cantidad_sacos_50kg' => $venta->Cant50kg, 

                        'precio_unitario' => $venta->PrecioVenta,
                        'precio_unitario_igv' => $venta->PrecioVentaIGV,
                        'importe_sin_igv' => $venta->Subtotal, 
                        'igv' => $venta->TotalVenta-$venta->Subtotal,
                        'importe_total_con_igv' => $venta->TotalVenta, 

                        'costo_unitario' => $venta->CostoUnitario,
                        'costo_total' => $venta->CostoExtendido, 

                        'margen' => $venta->Subtotal - $venta->CostoExtendido,
                        'margen_porcentaje' => (($venta->Subtotal - $venta->CostoExtendido)/$venta->Subtotal)*100, 

                        'precio_50kg' => $venta->P50Kg,
                        'costo_50kg' => $venta->C50Kg, 

                        'descuento_ivap' => $venta->DescuentoIvap,
                        'comision' => $venta->Comision, 
                        'tipo_descuento' => $venta->TipoDescuento,
                        'importe_descuento' => $venta->CantidadDescuento, 
                        'facturado' => $venta->FACT_BOL,

                    ];
                }




                /*
                |--------------------------------------------------------------------------
                | 6. Insertar en PostgreSQL dentro de transacción
                |--------------------------------------------------------------------------
                */
                DB::connection('pgsqlwin')->transaction(function () use ($insertData) {
                    DB::connection('pgsqlwin')
                        ->table('venta_arroz_costeada_raw')
                        ->insert($insertData);
                });

                $totalProcesados += count($insertData);

                echo "Procesados: " . $totalProcesados . "\n";
            });


        echo "Migración finalizada. Total: " . $totalProcesados . "\n";
    }


    private function tdventassincostos() {

        set_time_limit(0);

        echo "Inicio de migración...\n";

        /*
        |--------------------------------------------------------------------------
        | 1. Limpiar tabla destino (rápido)
        |--------------------------------------------------------------------------
        */
        DB::connection('pgsqlwin')->statement('TRUNCATE TABLE venta_arroz_raw RESTART IDENTITY');


        /*
        |--------------------------------------------------------------------------
        | 2. Procesar en chunks con orden estable
        |--------------------------------------------------------------------------
        */
        $totalProcesados = 0;

        DB::connection('sqlsrv_r')
            ->table('VentasConsolidado')
            ->orderBy('FECHA')
            ->orderBy('ORDEN') // Campo estable (ajusta si tienes un ID único)
            ->chunk(1000, function ($ventas) use (&$totalProcesados) {

                if ($ventas->isEmpty()) {
                    return;
                }

                /*
                |--------------------------------------------------------------------------
                | 3. Obtener productos del chunk
                |--------------------------------------------------------------------------
                */
                $codigosProducto = $ventas->pluck('COD_PRODUCTO')->unique()->toArray();

                $productosInfo = DB::connection('sqlsrv_actual')
                    ->table('ALM.PRODUCTO as P')
                    ->join('CMP.CATEGORIA as C', 'P.COD_CATEGORIA_TIPO_PRODUCTO', '=', 'C.COD_CATEGORIA')
                    ->whereIn('P.COD_PRODUCTO', $codigosProducto)
                    ->select(
                        'P.COD_PRODUCTO',
                        'P.CAN_PESO_MATERIAL',
                        'C.NOM_CATEGORIA as tipo_producto',
                        'P.IND_MATERIAL_SERVICIO'
                    )
                    ->get()
                    ->keyBy('COD_PRODUCTO')
                    ->toArray();


                /*
                |--------------------------------------------------------------------------
                | 4. Obtener clientes del chunk
                |--------------------------------------------------------------------------
                */
                $clientes = $ventas->pluck('COD_EMPR_CLIENTE')->unique()->toArray();

                $clientesRelacion = DB::connection('sqlsrv_actual')
                    ->table('STD.EMPRESA')
                    ->whereIn('COD_EMPR', $clientes)
                    ->pluck('IND_RELACIONADO', 'COD_EMPR')
                    ->toArray();


                /*
                |--------------------------------------------------------------------------
                | 5. Preparar inserción
                |--------------------------------------------------------------------------
                */
                $insertData = [];

                foreach ($ventas as $acopio) {

                    $producto = $productosInfo[$acopio->COD_PRODUCTO] ?? null;
                    $pesoKilogramos = $producto->CAN_PESO_MATERIAL ?? 0;

                    // Tipo cliente
                    $indRelacionado = $clientesRelacion[$acopio->COD_EMPR_CLIENTE] ?? 0;
                    $tipoVenta = ($indRelacionado == 0) ? 'TERCERO' : 'RELACIONADA';

                    // Tipo producto
                    $tipoProducto = $producto->tipo_producto ?? null;
                    if ($acopio->COD_FAMILIA_PRODUCTO == 'FAM0000000000062') {
                        $tipoProducto = 'PACAS';
                    }

                    $importe_sigv = $acopio->CAN_TOTAL_OV;
                    $igv = 0;
                    $importe = $acopio->CAN_TOTAL_OV;

                    if ($acopio->COD_EMP_OV == 'IACHEM0000001339') {
                        $importe_sigv = $acopio->CAN_TOTAL_OV/1.18;
                    }
                    // Material o servicio
                    $materialServicio = 'SERVICIO';
                    if ($producto && $producto->IND_MATERIAL_SERVICIO === 'M') {
                        $materialServicio = 'MATERIAL';
                    }
                    $cantidad = $acopio->CAN_PRODUCTO;
                    if ($acopio->ESTADO_ORDEN == 'ANULADA') {
                        $cantidad = 0;
                    }

                    $insertData[] = [

                        // NO enviamos id_venta → PostgreSQL lo genera

                        'empresa_codigo' => $acopio->COD_EMP_OV,
                        'empresa_nombre' => $acopio->NOM_EMPR,
                        'centro_codigo' => $acopio->COD_CENTRO,
                        'centro_nombre' => $acopio->CENTRO,
                        'fecha_venta' => date('Ymd', strtotime($acopio->FECHA)),
                        'numero_orden' => $acopio->ORDEN,
                        'estado_orden' => $acopio->ESTADO_ORDEN,
                        'cliente_codigo' => $acopio->COD_EMPR_CLIENTE,
                        'cliente_nombre' => $acopio->CLIENTE,
                        'tipo_cliente' => $tipoVenta,
                        'area_venta' => $acopio->TIPO_VENTA,
                        'jefe_venta_codigo' => $acopio->COD_CATEGORIA_JEFE_VENTA,
                        'jefe_venta_nombre' => $acopio->TXT_CATEGORIA_JEFE_VENTA,
                        'canal_codigo' => $acopio->COD_CATEGORIA_CANAL_VENTA,
                        'canal_venta' => $acopio->TXT_CATEGORIA_CANAL_VENTA,
                        'subcanal_codigo' => $acopio->COD_CATEGORIA_SUB_CANAL,
                        'subcanal_venta' => $acopio->TXT_CATEGORIA_SUB_CANAL,
                        'condicion_pago' => $acopio->TIPO_PAGO,
                        'moneda_codigo' => $acopio->COD_MONEDA,
                        'moneda' => $acopio->MONEDA,
                        'tipo_cambio' => $acopio->CAN_TIPO_CAMBIO,
                        'producto_codigo' => $acopio->COD_PRODUCTO,
                        'producto_nombre' => $acopio->NOM_PRODUCTO,
                        'tipo_item' => $materialServicio,
                        'tipo_producto' => $tipoProducto,
                        'familia_codigo' => $acopio->COD_FAMILIA_PRODUCTO,
                        'subfamilia_codigo' => $acopio->COD_SUBFAMILIA_PRODUCTO,
                        'subfamilia_nombre' => $acopio->NOM_SUBFAMILIA_PRODUCTO,
                        'unidad_medida' => $acopio->UM_OV,

                        'cantidad_unidades' => $cantidad,
                        'peso_kilogramos' => $pesoKilogramos,
                        'precio_unitario' => $acopio->CAN_PRECIO_UNIT,
                        'precio_unitario_igv' => $acopio->CAN_PRECIO_UNIT_IGV,

                        'importe_sin_igv' => $importe_sigv,
                        'igv' => $igv,
                        'importe_total_con_igv' => $importe,
                        'transferencia_gratuita' => $acopio->TRANFERENCIA_GRA,
                        'tipo_comprobante' => $acopio->FACT_BOL,
                    ];
                }




                /*
                |--------------------------------------------------------------------------
                | 6. Insertar en PostgreSQL dentro de transacción
                |--------------------------------------------------------------------------
                */
                DB::connection('pgsqlwin')->transaction(function () use ($insertData) {
                    DB::connection('pgsqlwin')
                        ->table('venta_arroz_raw')
                        ->insert($insertData);
                });

                $totalProcesados += count($insertData);

                echo "Procesados: " . $totalProcesados . "\n";
            });


        echo "Migración finalizada. Total: " . $totalProcesados . "\n";
    }



    private function tdacopio() {

        set_time_limit(0);
        DB::connection('pgsqlwin')->table('acopio_arroz_raw')->where('anio_compra','>=','2025')->delete();
        DB::connection('sqlsrv_r')
            ->table('adacopio_2025')
            ->orderBy('FEC_ORDEN')
            ->chunk(1000, function ($acopios) {
                $ultimoId = DB::connection('pgsqlwin')->table('acopio_arroz_raw')->max('id_acopio') ?? 0;
                $contador = $ultimoId + 1;

                // --- Obtener los CENTRSI únicos del chunk ---
                $centros = $acopios->pluck('COD_CENTRO')->unique()->toArray();

                // --- Consultar los tipos de venta (RELACIONADA o TERCERO) ---
                $centroRelacion = DB::connection('sqlsrv_actual')
                    ->table('ALM.CENTRO')
                    ->whereIn('COD_CENTRO', $centros)
                    ->pluck('NOM_CENTRO', 'COD_CENTRO')
                    ->toArray();

                $insertData = [];
                foreach ($acopios as $acopio) {

                    $centro_acopio_nombre = $centroRelacion[$acopio->COD_CENTRO] ?? '';

                    $insertData[] = [
                        'id_acopio' => $contador++,
                        'compra_codigo' => $acopio->COD_ORDEN,
                        'fecha_entrada' => date_format(date_create($acopio->FEC_ENTRADA), 'Ymd h:i:s'),
                        'fecha_compra' => date_format(date_create($acopio->FEC_ORDEN), 'Ymd h:i:s'),
                        'anio_compra' => $acopio->AÑO,
                        'mes_compra' => $acopio->MES,
                        'mes_numero_compra' => $acopio->MesNum,
                        'dia_numero_compra' => $acopio->DiaNum,
                        'empresa_codigo' => $acopio->COD_EMPR,
                        'empresa_nombre' => $acopio->Empresa,
                        'centro_acopio_codigo' => $acopio->COD_CENTRO,
                        'centro_acopio_nombre' => $centro_acopio_nombre,

                        'zona_comercial' => $acopio->ZONA,
                        'proveedor_codigo' => $acopio->COD_EMPR_CLIENTE,
                        'proveedor_nombre' => $acopio->PROVEEDOR,
                        'contrato_proveedor_codigo' => $acopio->COD_CONTRATO,
                        'tipo_contrato_codigo' => $acopio->COD_CATEGORIA_TIPO_CONTRATO,
                        'tipo_contrato_nombre' => $acopio->TIPO_CONTRATO,
                        'acopiador_nombre' => $acopio->ACOPIADOR,
                        'numero_carro' => $acopio->NRO_CARRO,
                        'lote_codigo' => $acopio->COD_LOTE,
                        'producto_codigo' => $acopio->COD_PRODUCTO,


                        'producto_nombre' => $acopio->Producto,
                        'variedad_arroz' => $acopio->VARIEDAD_RECEP,
                        'calidad' => $acopio->CALIDAD,
                        'cantidad_sacos' => $acopio->CAN_SACOS,
                        'precio_compra_kg' => $acopio->PRECIO_COMPRA,
                        'importe_total_compra' => $acopio->Importe,
                        'peso_humedo_kg' => $acopio->PESO_PLANTA_RECEP,
                        'peso_seco_kg' => $acopio->PesoSeco,
                        'tipo_compra' => $acopio->TipoCompra,
                        'humedad_porcentaje' => $acopio->HUMEDAD_RECEP,

                        'impurezas_porcentaje' => $acopio->IMPUREZAS_RECEP,
                        'estado_humedad' => $acopio->ESTADO_HS,
                        'grano_quebrado_porcentaje' => $acopio->CAN_Q,
                        'mancha_porcentaje' => $acopio->CAN_M,
                        'tiza_porcentaje' => $acopio->CAN_T,
                        'rendimiento_directo_porcentaje' => $acopio->CAN_RD,
                        'rendimiento_directo_pila_porcentaje' => $acopio->CAN_RDP,
                        'rendimiento_clasificado_porcentaje' => $acopio->CAN_RCLA,
                        'rendimiento_blanco_porcentaje' => $acopio->CAN_RB,
                        'precio_costo_recepcion_kg' => $acopio->PRECIO_COSTO_RECEP,
                        'cpus' => 0.00,
                        'precio_compra_inicial_kg' => $acopio->PrecioCompra,
                    ];
                }

                // Inserción masiva en PostgreSQL
                DB::connection('pgsqlwin')->table('acopio_arroz_raw')->insert($insertData);
        });
    }

	private function tdventas() {

		set_time_limit(0);
		DB::connection('pgsqlwin')->table('ventas')->delete();
        DB::connection('sqlsrv_r')
            ->table('VentasConsolidado2025')
            ->orderBy('ORDEN')
            ->chunk(1000, function ($ventas) {

                $ultimoId = DB::connection('pgsqlwin')->table('ventas')->max('id') ?? 0;
                $contador = $ultimoId + 1;

                // --- Obtener los COD_PRODUCTO únicos del chunk ---
                $codigosProducto = $ventas->pluck('COD_PRODUCTO')->unique()->toArray();

                // --- Consultar los pesos, tipo_producto y material_servicio ---
                $productosInfo = DB::connection('sqlsrv_actual')
                    ->table('ALM.PRODUCTO as P')
                    ->join('CMP.CATEGORIA as C', 'P.COD_CATEGORIA_TIPO_PRODUCTO', '=', 'C.COD_CATEGORIA')
                    ->whereIn('P.COD_PRODUCTO', $codigosProducto)
                    ->select('P.COD_PRODUCTO', 'P.CAN_PESO_MATERIAL', 'C.NOM_CATEGORIA as tipo_producto', 'P.IND_MATERIAL_SERVICIO')
                    ->get()
                    ->keyBy('COD_PRODUCTO')
                    ->toArray();

                // --- Obtener los clientes únicos del chunk ---
                $clientes = $ventas->pluck('COD_EMPR_CLIENTE')->unique()->toArray();

                // --- Consultar los tipos de venta (RELACIONADA o TERCERO) ---
                $clientesRelacion = DB::connection('sqlsrv_actual')
                    ->table('STD.EMPRESA')
                    ->whereIn('COD_EMPR', $clientes)
                    ->pluck('IND_RELACIONADO', 'COD_EMPR')
                    ->toArray();

                $insertData = [];
                foreach ($ventas as $venta) {

                    // Info de producto
                    $producto = $productosInfo[$venta->COD_PRODUCTO] ?? null;
                    $pesoKilogramos = $producto->CAN_PESO_MATERIAL ?? 0;
                    $tipoProducto = $producto->tipo_producto ?? null;

                    // Si IND_MATERIAL_SERVICIO = 'M' → MATERIAL, si es null/vacío → SERVICIO
                    $materialServicio = null;
                    if ($producto) {
                        $materialServicio = (isset($producto->IND_MATERIAL_SERVICIO) && $producto->IND_MATERIAL_SERVICIO === 'M')
                            ? 'MATERIAL'
                            : 'SERVICIO';
                    } else {
                        $materialServicio = 'SERVICIO'; // Si no existe el producto
                    }

                    // Tipo de venta (RELACIONADA o TERCERO)
                    $indRelacionado = $clientesRelacion[$venta->COD_EMPR_CLIENTE] ?? 0;
                    $tipoVenta = ($indRelacionado == 0) ? 'TERCERO' : 'RELACIONADA';

                    $insertData[] = [
                        'id' => $contador++,
                        'empresa_id' => $venta->COD_EMP_OV,
                        'empresa_nombre' => $venta->NOM_EMPR,
                        'centro_id' => $venta->COD_CENTRO,
                        'centro_nombre' => $venta->CENTRO,
                        'fecha' => $venta->FECHA,
                        'venta_id' => $venta->ORDEN,
                        'estado' => $venta->ESTADO_ORDEN,
                        'cliente_id' => $venta->COD_EMPR_CLIENTE,
                        'cliente_nombre' => $venta->CLIENTE,
                        'contrato_id' => $venta->REG_COMERCIAL,
                        'tipo_venta_id' => $venta->COD_TIPO_VENTA,
                        'tipo_venta_nombre' => $venta->TIPO_VENTA,
                        'moneda_id' => $venta->COD_MONEDA,
                        'moneda_nombre' => $venta->MONEDA,
                        'tipo_cambio' => $venta->CAN_TIPO_CAMBIO,
                        'vendedor_id' => $venta->COD_CATEGORIA_JEFE_VENTA,
                        'vendedor_nombre' => $venta->TXT_CATEGORIA_JEFE_VENTA,
                        'canal_id' => $venta->COD_CATEGORIA_CANAL_VENTA,
                        'canal_nombre' => $venta->TXT_CATEGORIA_CANAL_VENTA,
                        'sub_canal_id' => $venta->COD_CATEGORIA_SUB_CANAL,
                        'sub_canal_nombre' => $venta->TXT_CATEGORIA_SUB_CANAL,
                        'tipo_pago' => $venta->TIPO_PAGO,
                        'pedido_mobil_id' => $venta->COD_UM_OV,
                        'unidad_medida' => $venta->UM_OV,
                        'producto_id' => $venta->COD_PRODUCTO,
                        'indicador_gratuito' => $venta->COD_TIPO_PRODUCTO,
                        'familia_producto_id' => $venta->COD_FAMILIA_PRODUCTO,
                        'sub_familia_producto_id' => $venta->COD_SUBFAMILIA_PRODUCTO,
                        'sub_familia_producto_nombre' => $venta->NOM_SUBFAMILIA_PRODUCTO,
                        'producto_nombre' => $venta->NOM_PRODUCTO,
                        'cantidad' => $venta->CAN_PRODUCTO,
                        'kilogramos' => $venta->KG_OV,
                        'precio_unitario' => $venta->CAN_PRECIO_UNIT,
                        'precio_unitario_igv' => $venta->CAN_PRECIO_UNIT_IGV,
                        'total' => $venta->CAN_TOTAL_OV,
                        'documento_interno' => $venta->DIV,
                        'transferencia_gratuita' => $venta->TRANFERENCIA_GRA,
                        'indicador_facturado' => $venta->FACT_BOL,
                        'peso_kilogramos' => $pesoKilogramos,
                        'tipo_producto' => $tipoProducto,
                        'material_servicio' => $materialServicio,
                        'tipo_venta' => $tipoVenta,
                    ];
                }

                // Inserción masiva en PostgreSQL
                DB::connection('pgsqlwin')->table('ventas')->insert($insertData);
        });
    }

	private function tdventasatendidas() {

			set_time_limit(0);
			DB::connection('pgsqla')->table('ventas')->delete();

			DB::table('viewVentaSalidas2024 as vvs')
			    ->leftJoin(DB::raw("(SELECT TOP 1  ALM.PRODUCTO.* FROM ALM.PRODUCTO) AS p"),
			                function ($join) {
			                    $join->on('p.NOM_PRODUCTO', '=', 'vvs.NombreProducto');
			                })
			    ->leftJoin('CMP.CATEGORIA as MARCA', 'MARCA.COD_CATEGORIA', '=', 'p.COD_CATEGORIA_MARCA')
			    ->leftJoin('CMP.CATEGORIA as TIPOMARCA', 'TIPOMARCA.COD_CATEGORIA', '=', 'p.COD_CATEGORIA_PRODUCTO_SUPERMERCADOS')
			    ->whereRaw("ISNULL(vvs.NombreProducto, '') <> ''")
			    ->select('vvs.*', 
			             'MARCA.NOM_CATEGORIA as Marca', 
			             'TIPOMARCA.NOM_CATEGORIA as TipoMarca',
			             DB::raw('YEAR(Fecha) as Anio'),
			             DB::raw('MONTH(Fecha) AS Mes'),
			             DB::raw('DAY(Fecha) AS Dia'),
			             DB::raw('CantidadProducto2 * PrecioVentaIGV as Venta'))
			    ->orderBy('vvs.Fecha', 'asc')
			    ->orderBy('vvs.Orden', 'asc')
			    ->chunk(1500, function ($datos) {
			        $datosParaInsertar = [];
			        
			        foreach ($datos as $dato) {
			            $empresa = $dato->Empresa;
			            if($dato->Empresa == 'INDUAMERICA COMERCIAL SAC'){
			                $empresa = 'INDUAMERICA COMERCIAL SOCIEDAD ANONIMA CERRADA';
			            }
			            
			            $datosParaInsertar[] = [
			            	'orden' 	=> $dato->Orden,
			                'empresa' 	=> $empresa,
			                'centro' 	=> $dato->Centro,
			                'fecha'  	=> $dato->Fecha,
			                'anio'  	=> $dato->Anio,
			                'mes'  		=> $dato->Mes,
			                'dia'  		=> $dato->Dia,
			                'cliente' 	=> $dato->Cliente,
			                'canal' 	=> $dato->Canal,
			                'subcanal'  => $dato->SubCanal,
			                'vendedor'  => $dato->JefeVenta,
			                'estado' 	=> $dato->Estado,
			                'producto'  => $dato->NombreProducto,
			                'familia'   => $dato->Familia,
			                'subfamilia'=> $dato->SubFamilia,
			                'marca'     => $dato->Marca,
			                'cantidad'  => $dato->CantidadProducto,
			                'costo_unitario'  => $dato->CostoUnitario,
			                'precio'    => $dato->PrecioVenta,
			                'total'  	=> $dato->Venta,
			                'kilogramos'=> $dato->Kg,
			                'sacos_cincuenta_kilogramos' => $dato->Cant50kg
			            ];
			        }
			        
			        DB::connection('pgsqla')->table('ventas')->insert($datosParaInsertar);
			    });

			    return "Transferencia completada con éxito.";
		        $FeToken = FeToken::get();
		        $results = DB::connection('pgsqla')->select('SELECT * FROM ventas');
		        dd($results);
		        dd($FeToken);
	}

}