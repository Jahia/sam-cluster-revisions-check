package org.jahia.community.sam.clusterrevisionscheck;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import org.apache.jackrabbit.core.util.db.ConnectionHelper;
import org.apache.jackrabbit.core.util.db.DbUtility;
import org.jahia.modules.sam.Probe;
import org.jahia.modules.sam.ProbeStatus;
import org.osgi.service.component.annotations.Component;

import org.jahia.modules.sam.ProbeSeverity;
import org.jahia.utils.DatabaseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(immediate = true, service = Probe.class)
public class ClusterRevisionsProbe implements Probe {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterRevisionsProbe.class);
    private static final String PROP_SCHEMA_OBJECT_PREFIX = "schemaObjectPrefix";
    private static final String PROP_CHECK_INTERVAL = "checkInterval";
    private Long lastRevisionId = Long.MAX_VALUE;
    private String lastJournalId = "";
    private String schemaObjectPrefix = "JR_J_";
    private String sqlStmtSelectGlobalRevision;
    private String sqlStmtSelectMinimumRevision;
    private final ConnectionHelper connHelper;
    private long checkInterval = 15L;
    private long lastCheckTimestamp = Long.MIN_VALUE;

    public ClusterRevisionsProbe() {
        connHelper = new ConnectionHelper(DatabaseUtils.getDatasource(), false);
    }

    @Override
    public String getName() {
        return "ClusterRevisionsCheck";
    }

    @Override
    public String getDescription() {
        return "Checking that the cluster revisions aren't out-of-sync";
    }

    @Override
    public ProbeStatus getStatus() {
        ProbeStatus status = new ProbeStatus("Cluster revisions are in-sync", ProbeStatus.Health.GREEN);
        final long currentCheckTimestamp = System.currentTimeMillis();

        if (currentCheckTimestamp >= lastCheckTimestamp + checkInterval * 1000L) {
            lastCheckTimestamp = currentCheckTimestamp;
            try {
                long globalRevision = getQueryLong(sqlStmtSelectGlobalRevision);
                ResultSet rs = null;
                try {
                    rs = connHelper.exec(sqlStmtSelectMinimumRevision, new Object[]{globalRevision}, false, 1);
                    if (rs.next()) {
                        final String journalId = rs.getString(1);
                        final Long revisionId = rs.getLong(2);
                        if (journalId.equals(lastJournalId) && revisionId.equals(lastRevisionId)) {
                            status = new ProbeStatus(String.format("The following node seems out-of-sync: %s (%s)", lastJournalId, lastRevisionId.toString()), ProbeStatus.Health.RED);
                        } else {
                            lastJournalId = journalId;
                            lastRevisionId = revisionId; 
                        }
                    }
                } finally {
                    DbUtility.close(rs);
                }
            } catch (SQLException ex) {
                LOGGER.error("Issue when executing SQL statement", ex);
            }

        }
        return status;
    }

    @Override
    public ProbeSeverity getDefaultSeverity() {
        return ProbeSeverity.HIGH;
    }

    @Override
    public void setConfig(Map<String, Object> config) {
        if (config.containsKey(PROP_CHECK_INTERVAL)) {
            checkInterval = Long.parseLong(config.get(PROP_CHECK_INTERVAL).toString());
        }
        if (config.containsKey(PROP_SCHEMA_OBJECT_PREFIX)) {
            schemaObjectPrefix = config.get(PROP_SCHEMA_OBJECT_PREFIX).toString();
        }
        sqlStmtSelectGlobalRevision = "SELECT REVISION_ID FROM " + schemaObjectPrefix + "GLOBAL_REVISION";
        sqlStmtSelectMinimumRevision = "SELECT JOURNAL_ID, REVISION_ID FROM " + schemaObjectPrefix + "LOCAL_REVISIONS WHERE REVISION_ID < ? ORDER BY REVISION_ID ASC";
    }

    private long getQueryLong(String query, Object... params) throws SQLException {
        ResultSet rs = null;
        long result = 0;
        try {
            rs = connHelper.exec(query, params, false, 0);
            if (rs.next()) {
                result = rs.getLong(1);
            }
        } finally {
            DbUtility.close(rs);
        }
        return result;
    }

}
