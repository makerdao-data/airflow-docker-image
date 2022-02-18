from datetime import datetime, timedelta
from snowflake.connector import connect
import os
from random import randint
from dags.connectors.sf import sf
from dags.connectors.sf import _write_to_stage, _write_to_table, _clear_stage


def _history():

    start = sf.execute("""
        select max(day)
        from maker.history.vaults;
    """).fetchone()[0]

    if start:
        start = start + timedelta(days=1)
    else:
        start = sf.execute("""
            select min(timestamp)
            from mcd.public.vaults;
        """).fetchone()[0]

    end = datetime.utcnow() - timedelta(days=1)

    days_to_process = list()
    days_to_process.append(start.date())
    while start.date() < end.date():

        d = start + timedelta(days=1)
        days_to_process.append(d.date())
        start = d

    output = list()

    for day in days_to_process:

        vaults_sum = sf.execute(f"""
            select vault, ilk, round(sum(dcollateral), 8), round(sum(dprincipal), 8), sum(dart / power(10,18)), sum(dfees)
            from mcd.public.vaults
            where date(timestamp) <= '{day}'
            group by vault, ilk
            order by vault, ilk;
        """).fetchall()

        vaults_day = sf.execute(f"""
            select vault, ilk, round(dcollateral, 8), dart / power(10,18), round(dprincipal, 8), dfees
            from mcd.public.vaults
            where date(timestamp) = '{day}'
            order by vault, ilk;
        """).fetchall()

        daily_operations = dict()

        for vault, ilk, dcollateral, dart, dprincipal, dfees in vaults_day:

            daily_operations.setdefault(str(vault), {})
            daily_operations[str(vault)].setdefault(ilk, dict(
                deposit=0,
                withdraw=0,
                debt_generate=0,
                debt_payback=0,
                principal_generate=0,
                principal_payback=0,
                fees=0
            ))

            if dcollateral < 0:

                daily_operations[str(vault)][ilk]['withdraw'] += dcollateral
            
            if dcollateral > 0:

                daily_operations[str(vault)][ilk]['deposit'] += dcollateral
            
            if dart > 0:

                daily_operations[str(vault)][ilk]['debt_generate'] += dart
            
            if dart < 0:

                daily_operations[str(vault)][ilk]['debt_payback'] += dart
            
            if dprincipal > 0:

                daily_operations[str(vault)][ilk]['principal_generate'] += dprincipal
            
            if dprincipal < 0:

                daily_operations[str(vault)][ilk]['principal_payback'] += dprincipal
            
            if dfees != 0:

                daily_operations[str(vault)][ilk]['fees'] += dfees

        
        for vault, ilk, dcollateral, dprincipal, dart, dfees in vaults_sum:

            item = [
                day,
                vault,
                ilk,
                dcollateral,
                dprincipal,
                dart,
                dfees
            ]

            item_details = [0, 0, 0, 0, 0, 0, 0]
            if vault in daily_operations:
                if ilk in daily_operations[str(vault)]:
                    item_details = [
                        daily_operations[str(vault)][ilk]['withdraw'],
                        daily_operations[str(vault)][ilk]['deposit'],
                        daily_operations[str(vault)][ilk]['debt_generate'],
                        daily_operations[str(vault)][ilk]['debt_payback'],
                        daily_operations[str(vault)][ilk]['principal_generate'],
                        daily_operations[str(vault)][ilk]['principal_payback'],
                        daily_operations[str(vault)][ilk]['fees']
                    ]
            
            output.append(item + item_details)

    if output:
        pattern = _write_to_stage(sf, output, f"mcd.staging.vaults_extracts")
        if pattern:
            _write_to_table(
                sf,
                f"mcd.staging.vaults_extracts",
                f"maker.history.vaults",
                pattern,
            )
            _clear_stage(sf, f"mcd.staging.vaults_extracts", pattern)
    
    return
