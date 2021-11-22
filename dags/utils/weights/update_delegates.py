from dags.connectors.chain import chain
from dags.connectors.sf import sf
from dags.utils.weights.get_delegate_details import _get_delegate_details


def _update_delegates():
    for vote_delegate, type, name in sf.execute("""
        SELECT vote_delegate, type, name
        FROM delegates.public.delegates
        WHERE name IS NULL;
    """
    ).fetchall():

        if not type and not name:
            
            name, type = _get_delegate_details(chain, vote_delegate)

            if not name:
                sf.execute(f"""
                    UPDATE delegates.public.delegates
                    SET type = '{type}'
                    WHERE vote_delegate = '{vote_delegate}';
                """)
            else:
                sf.execute(f"""
                    UPDATE delegates.public.delegates
                    SET type = '{type}', name = '{name}'
                    WHERE vote_delegate = '{vote_delegate}';
                """)
        elif type and not name:

            name, type = _get_delegate_details(chain, vote_delegate)

            if name:
                
                sf.execute(f"""
                    UPDATE delegates.public.delegates
                    SET type = '{type}', name = '{name}'
                    WHERE vote_delegate = '{vote_delegate}';
                """)
        
        else:
            
            pass
    
    return
