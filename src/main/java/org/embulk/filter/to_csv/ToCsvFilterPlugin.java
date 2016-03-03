package org.embulk.filter.to_csv;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.embulk.spi.util.Newline;
import org.embulk.spi.util.Timestamps;
import org.msgpack.value.Value;
import org.slf4j.Logger;

import java.util.Map;

public class ToCsvFilterPlugin
        implements FilterPlugin
{
    public enum QuotePolicy
    {
        ALL("ALL"),
        MINIMAL("MINIMAL"),
        NONE("NONE");

        private final String string;

        QuotePolicy(String string)
        {
            this.string = string;
        }

        public String getString()
        {
            return string;
        }
    }

    public interface TimestampColumnOption
            extends Task, TimestampFormatter.TimestampColumnOption
    {
    }

    public interface PluginTask
            extends Task, TimestampFormatter.Task
    {
        @Config("column_name")
        @ConfigDefault("\"payload\"")
        String getColumnName();

        @Config("header_line")
        @ConfigDefault("true")
        boolean getHeaderLine();

        @Config("delimiter")
        @ConfigDefault("\",\"")
        char getDelimiterChar();

        @Config("quote")
        @ConfigDefault("\"\\\"\"")
        char getQuoteChar();

        @Config("quote_policy")
        @ConfigDefault("\"MINIMAL\"")
        QuotePolicy getQuotePolicy();

        @Config("escape")
        @ConfigDefault("null")
        Optional<Character> getEscapeChar();

        @Config("null_string")
        @ConfigDefault("\"\"")
        String getNullString();

        @Config("newline_in_field")
        @ConfigDefault("\"LF\"")
        Newline getNewlineInField();

        @Config("column_options")
        @ConfigDefault("{}")
        Map<String, TimestampColumnOption> getColumnOptions();
    }

    private final Logger logger = Exec.getLogger(ToCsvFilterPlugin.class);
    private final static int INDEX = 0;
    private final static Type TYPE = Types.STRING;


    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        // validate column_options
        for (String columnName : task.getColumnOptions().keySet()) {
            inputSchema.lookupColumn(columnName);  // throws SchemaConfigException
        }

        Schema outputSchema = new Schema(ImmutableList.of(new Column(INDEX, task.getColumnName(), TYPE)));

        control.run(task.dump(), outputSchema);
    }

    @Override
    public PageOutput open(TaskSource taskSource, final Schema inputSchema,
            final Schema outputSchema, final PageOutput output)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);
        final TimestampFormatter[] timestampFormatters = Timestamps.newTimestampColumnFormatters(task, outputSchema, task.getColumnOptions());
        final char delimiter = task.getDelimiterChar();
        final QuotePolicy quotePolicy = task.getQuotePolicy();
        final char quote = task.getQuoteChar() != '\0' ? task.getQuoteChar() : '"';
        final char escape = task.getEscapeChar().or(quotePolicy == QuotePolicy.NONE ? '\\' : quote);
        final String nullString = task.getNullString();
        final String newlineInField = task.getNewlineInField().getString();
        final boolean writeHeaderLine = task.getHeaderLine();

        return new PageOutput() {
            private final PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);
            private final PageReader pageReader = new PageReader(inputSchema);
            private final Column outputColumn = outputSchema.getColumn(INDEX);
            private final String delimiterString = String.valueOf(delimiter);
            private boolean shouldWriteHeaderLine = writeHeaderLine;
            private StringBuilder lineBuilder = new StringBuilder();

            @Override
            public void add(Page page)
            {
                writeHeader();

                pageReader.setPage(page);
                while (pageReader.nextRecord()) {
                    outputSchema.visitColumns(new ColumnVisitor() {
                        @Override
                        public void booleanColumn(Column column)
                        {
                            addDelimiter(column);
                            if (!pageReader.isNull(column)) {
                                addValue(Boolean.toString(pageReader.getBoolean(column)));
                            } else {
                                addNullString();
                            }
                        }

                        @Override
                        public void longColumn(Column column)
                        {
                            addDelimiter(column);
                            if (!pageReader.isNull(column)) {
                                addValue(Long.toString(pageReader.getLong(column)));
                            } else {
                                addNullString();
                            }
                        }

                        @Override
                        public void doubleColumn(Column column)
                        {
                            addDelimiter(column);
                            if (!pageReader.isNull(column)) {
                                addValue(Double.toString(pageReader.getDouble(column)));
                            } else {
                                addNullString();
                            }
                        }

                        @Override
                        public void stringColumn(Column column)
                        {
                            addDelimiter(column);
                            if (!pageReader.isNull(column)) {
                                addValue(pageReader.getString(column));
                            } else {
                                addNullString();
                            }
                        }

                        @Override
                        public void timestampColumn(Column column)
                        {
                            addDelimiter(column);
                            if (!pageReader.isNull(column)) {
                                Timestamp value = pageReader.getTimestamp(column);
                                addValue(timestampFormatters[column.getIndex()].format(value));
                            } else {
                                addNullString();
                            }
                        }

                        @Override
                        public void jsonColumn(Column column)
                        {
                            addDelimiter(column);
                            if (!pageReader.isNull(column)) {
                                Value value = pageReader.getJson(column);
                                addValue(value.toJson());
                            } else {
                                addNullString();
                            }
                        }


                        private void addDelimiter(Column column)
                        {
                            if (column.getIndex() != 0) {
                                lineBuilder.append(delimiterString);
                            }
                        }

                        private void addValue(String v)
                        {
                            lineBuilder.append(setEscapeAndQuoteValue(v, delimiter, quotePolicy, quote, escape, newlineInField, nullString));
                        }

                        private void addNullString()
                        {
                            lineBuilder.append(nullString);
                        }
                    });

                    addRecord();
                }
            }

            @Override
            public void finish()
            {
                pageBuilder.finish();
            }

            @Override
            public void close()
            {
                pageBuilder.close();
            }

            private void addRecord()
            {
                pageBuilder.setString(outputColumn, lineBuilder.toString());
                lineBuilder = new StringBuilder();
                pageBuilder.addRecord();
            }

            private void writeHeader()
            {
                if (!shouldWriteHeaderLine) {
                    return;
                }

                for (Column column : pageReader.getSchema().getColumns()) {
                    if (column.getIndex() != 0) {
                        lineBuilder.append(delimiterString);
                    }
                    lineBuilder.append(setEscapeAndQuoteValue(column.getName(), delimiter, quotePolicy, quote, escape, newlineInField, nullString));
                }
                addRecord();
                shouldWriteHeaderLine = false;
            }
        };
    }

    private String setEscapeAndQuoteValue(String v, char delimiter, QuotePolicy policy, char quote, char escape, String newline, String nullString)
    {
        StringBuilder escapedValue = new StringBuilder();
        char previousChar = ' ';

        boolean isRequireQuote = (policy == QuotePolicy.ALL || policy == QuotePolicy.MINIMAL && v.equals(nullString));

        for (int i = 0; i < v.length(); i++) {
            char c = v.charAt(i);

            if (policy != QuotePolicy.NONE && c == quote) {
                escapedValue.append(escape);
                escapedValue.append(c);
                isRequireQuote = true;
            } else if (c == '\r') {
                if (policy == QuotePolicy.NONE) {
                    escapedValue.append(escape);
                }
                escapedValue.append(newline);
                isRequireQuote = true;
            } else if (c == '\n') {
                if (previousChar != '\r') {
                    if (policy == QuotePolicy.NONE) {
                        escapedValue.append(escape);
                    }
                    escapedValue.append(newline);
                    isRequireQuote = true;
                }
            } else if (c == delimiter) {
                if (policy == QuotePolicy.NONE) {
                    escapedValue.append(escape);
                }
                escapedValue.append(c);
                isRequireQuote = true;
            } else {
                escapedValue.append(c);
            }
            previousChar = c;
        }

        if (policy != QuotePolicy.NONE && isRequireQuote) {
            return setQuoteValue(escapedValue.toString(), quote);
        } else {
            return escapedValue.toString();
        }
    }

    private String setQuoteValue(String v, char quote)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(quote);
        sb.append(v);
        sb.append(quote);

        return sb.toString();
    }
}
