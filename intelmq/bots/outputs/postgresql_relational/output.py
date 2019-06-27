# -*- coding: utf-8 -*-
"""
PostgreSQL output bot.

See Readme.md for installation and configuration.

In case of errors, the bot tries to reconnect if the error is of operational
and thus temporary. We don't want to catch too much, like programming errors
(missing fields etc).
"""

from intelmq.lib.bot import Bot

try:
    import psycopg2
except ImportError:
    psycopg2 = None


class PostgreSQLRelationalOutputBot(Bot):

    def init(self):
        self.logger.debug("Connecting to PostgreSQL.")
        if psycopg2 is None:
            raise ValueError("Could not import 'psycopg2'. Please install it.")

        try:
            if hasattr(self.parameters, 'connect_timeout'):
                connect_timeout = self.parameters.connect_timeout
            else:
                connect_timeout = 5

            self.con = psycopg2.connect(database=self.parameters.database,
                                        user=self.parameters.user,
                                        password=self.parameters.password,
                                        host=self.parameters.host,
                                        port=self.parameters.port,
                                        sslmode=self.parameters.sslmode,
                                        connect_timeout=connect_timeout,
                                        )
            self.cur = self.con.cursor()
            self.con.autocommit = getattr(self.parameters, 'autocommit', True)

            self.jsondict_as_string = getattr(self.parameters, 'jsondict_as_string', True)
        except Exception:
            self.logger.exception('Failed to connect to database.')
            raise
        self.logger.info("Connected to PostgreSQL.")

    def insert_or_retrieve_classification(self, classification, taxonomy):
        query = ("""WITH new_row AS ( INSERT INTO classification_type(taxonomy, type, official)
                                    SELECT %(taxonomy)s, %(classification)s, false
                                    WHERE NOT EXISTS (SELECT * FROM classification_type WHERE type = %(classification)s AND taxonomy = %(taxonomy)s)
                                    RETURNING id
                                  )
                                  SELECT id FROM new_row
                                  UNION
                                  SELECT id FROM classification_type WHERE type = %(classification)s AND taxonomy = %(taxonomy)s """)
        id = None
        try:
            self.cur.execute(query,{'classification':classification,'taxonomy':taxonomy})
            id = self.cur.fetchone()[0]
        except (psycopg2.InterfaceError, psycopg2.InternalError,
                psycopg2.OperationalError, AttributeError):
            self.logger.exception('failure adding classification')
            try:
                self.con.rollback()
                self.logger.exception('Executed rollback command '
                                      'after failed query execution.')
            except psycopg2.OperationalError:
                self.logger.exception('Executed rollback command '
                                      'after failed query execution.')
                self.init()
            except Exception:
                self.logger.exception('Cursor has been closed, connecting '
                                      'again.')
                self.init()
        return id

    def insert_or_retrieve_feed_provider(self, feed_provider):
        query = ("""WITH new_row AS ( INSERT INTO feed_provider(name)
                                    SELECT %(feed_provider)s
                                    WHERE NOT EXISTS (SELECT * FROM feed_provider WHERE name = %(feed_provider)s)
                                    RETURNING id
                                  )
                                  SELECT id FROM new_row
                                  UNION
                                  SELECT id FROM feed_provider WHERE name = %(feed_provider)s """)
        id = None
        try:
            self.cur.execute(query,{'feed_provider':feed_provider})
            id = self.cur.fetchone()[0]
        except (psycopg2.InterfaceError, psycopg2.InternalError,
                psycopg2.OperationalError, AttributeError):
            self.logger.exception('failure adding ip from feed')
            try:
                self.con.rollback()
                self.logger.exception('Executed rollback command '
                                      'after failed query execution.')
            except psycopg2.OperationalError:
                self.logger.exception('Executed rollback command '
                                      'after failed query execution.')
                self.init()
            except Exception:
                self.logger.exception('Cursor has been closed, connecting '
                                      'again.')
                self.init()
        return id


    def insert_or_retrieve_feed(self, feed, feed_provider):
        provider_id = self.insert_or_retrieve_feed_provider(feed_provider)
        if provider_id is None:
             return None
        query = ("""WITH new_row AS ( INSERT INTO feed(name, feed_provider)
                                    SELECT %(feed)s, %(feed_provider)s
                                    WHERE NOT EXISTS (SELECT * FROM feed WHERE name = %(feed)s AND feed_provider = %(feed_provider)s)
                                    RETURNING id
                                  )
                                  SELECT id FROM new_row
                                  UNION
                                  SELECT id FROM feed WHERE name = %(feed)s AND feed_provider = %(feed_provider)s""")
        id = None
        try:
            self.cur.execute(query,{'feed':feed, 'feed_provider':provider_id})
            id = self.cur.fetchone()[0]
        except (psycopg2.InterfaceError, psycopg2.InternalError,
                psycopg2.OperationalError, AttributeError):
            self.logger.exception('failure adding feed')
            try:
                self.con.rollback()
                self.logger.exception('Executed rollback command '
                                      'after failed query execution.')
            except psycopg2.OperationalError:
                self.logger.exception('Executed rollback command '
                                      'after failed query execution.')
                self.init()
            except Exception:
                self.logger.exception('Cursor has been closed, connecting '
                                      'again.')
                self.init()
        return id

    def add_domain(self, domain):
        query = ("""WITH new_row AS ( INSERT INTO domain("domain", md5)
                                    SELECT %(domain)s, md5(%(domain)s)::uuid
                                    WHERE NOT EXISTS (SELECT * FROM domain WHERE "md5" = md5(%(domain)s)::uuid)
                                    RETURNING id
                                  )
                                  SELECT id FROM new_row
                                  UNION
                                  SELECT id FROM domain WHERE "md5" = md5(%(domain)s)::uuid """)
        id = None
        try:
            self.cur.execute(query,{'domain':domain})
            id = self.cur.fetchone()[0]
        except (psycopg2.InterfaceError, psycopg2.InternalError,
                psycopg2.OperationalError, AttributeError):
            self.logger.exception('failure adding domain')
            try:
                self.con.rollback()
                self.logger.exception('Executed rollback command '
                                      'after failed query execution.')
            except psycopg2.OperationalError:
                self.logger.exception('Executed rollback command '
                                      'after failed query execution.')
                self.init()
            except Exception:
                self.logger.exception('Cursor has been closed, connecting '
                                      'again.')
                self.init()
        return id

   
    def add_domain_from_feed(self, domain, feed, feed_provider, classification):
        feed_id = self.insert_or_retrieve_feed(feed, feed_provider)
        if feed_id is None:
            return False
        domain_id = self.add_domain(domain)
        if feed_id is None:
            return False

        query = ("""INSERT INTO feed_domain(feed_id, domain_id, status, classification)
                                    VALUES ( %(feed_id)s, %(domain_id)s, 'Active', %(classification)s)
                  ON CONFLICT(feed_id, domain_id)
                  DO UPDATE 
                SET last_seen = CURRENT_TIMESTAMP""")
        try:
            self.cur.execute(query,{'feed_id':feed_id, 'domain_id':domain_id,'classification':classification})
        except (psycopg2.InterfaceError, psycopg2.InternalError,
                psycopg2.OperationalError, AttributeError):
            self.logger.exception('failure adding domain from feed')
            try:
                self.con.rollback()
                self.logger.exception('Executed rollback command '
                                      'after failed query execution.')
            except psycopg2.OperationalError:
                self.logger.exception('Executed rollback command '
                                      'after failed query execution.')
                self.init()
            except Exception:
                self.logger.exception('Cursor has been closed, connecting '
                                      'again.')
                self.init()
        else:
            return True
        return False # return false if an exception occured
   
    def add_ip_from_feed(self, ip, feed, feed_provider, classification):
        feed_id = self.insert_or_retrieve_feed(feed, feed_provider)
        if feed_id is None:
            return False 
        query = ("""INSERT INTO feed_ip(feed_id, ip, status, classification)
                                    VALUES ( %(feed_id)s, %(ip)s, 'Active', %(classification)s)
                  ON CONFLICT(feed_id, ip)
                  DO UPDATE 
                        SET last_seen = CURRENT_TIMESTAMP""")
        try:
            self.cur.execute(query,{'feed_id':feed_id, 'ip':ip, 'classification':classification})
        except (psycopg2.InterfaceError, psycopg2.InternalError,
                psycopg2.OperationalError, AttributeError):
            self.logger.exception('failure adding ip from feed')
            try:
                self.con.rollback()
                self.logger.exception('Executed rollback command '
                                      'after failed query execution.')
            except psycopg2.OperationalError:
                self.logger.exception('Executed rollback command '
                                      'after failed query execution.')
                self.init()
            except Exception:
                self.logger.exception('Cursor has been closed, connecting '
                                      'again.')
                self.init()
        else:
            return True
        return False # return false if an exception occured

    def process(self):
        event = self.receive_message().to_dict(jsondict_as_string=self.jsondict_as_string)

        if not 'feed.provider' in event:
            self.logger.info('message has no feed provider')
        feed_provider = event.get('feed.provider')
        feed = event.get('feed.name')
        if 'classification.taxonomy' in event:
            taxonomy = event.get('classification.taxonomy')
            classification_type = event.get('classification.type')
        elif 'extra.classification_type' in event: # handle classification types that are not part of standard data harmonization
            taxonomy = 'other'
            classification_type = event.get('extra.classification_type')

        classification = self.insert_or_retrieve_classification(classification_type, taxonomy)
        if classification is None:
            return # dump out if we failed to get a classification 

        success = False
        if 'source.ip' in event:
             source_ip = event.get('source.ip')
             success = self.add_ip_from_feed(source_ip, feed, feed_provider, classification)
             if not success:
                 self.logger.warn('There was an error processing IP for %s.' % source_ip)
        elif 'source.network' in event:
             source_network = event.get('source.network')
             success = self.add_ip_from_feed(source_network, feed, feed_provider, classification)
        elif 'source.fqdn' in event:
             source_domain = event.get('source.fqdn')
             success = self.add_domain_from_feed(source_domain, feed, feed_provider, classification)
             if not success:
                 self.logger.warn('There was an error processing domain for %s.' % source_domain)
        else:
             self.logger.warn('No threat content for event from %s %s.' % (feed, feed_provider))
             success = True

        if success:
             self.con.commit()
             self.acknowledge_message()

BOT = PostgreSQLRelationalOutputBot
