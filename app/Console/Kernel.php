<?php

namespace App\Console;
use App\Console\Commands\NotificacionOC;
use Illuminate\Console\Scheduling\Schedule;
use Illuminate\Foundation\Console\Kernel as ConsoleKernel;

class Kernel extends ConsoleKernel
{
    /**
     * The Artisan commands provided by your application.
     *
     * @var array
     */
    protected $commands = [
        NotificacionOC::class
    ];

    /**
     * Define the application's command schedule.
     *
     * @param  \Illuminate\Console\Scheduling\Schedule  $schedule
     * @return void
     */
    protected function schedule(Schedule $schedule)
    {
        $schedule->command('notificacion:oc')->dailyAt('07:30');   // CADA MINUTO
        $schedule->command('notificacion:oc')->dailyAt('08:00');   // CADA MINUTO
        //$schedule->command('notificacion:oc')->everyMinute();
    }
    /**
     * Register the Closure based commands for the application.
     *
     * @return void
     */
    protected function commands()
    {
        require base_path('routes/console.php');
    }
}
