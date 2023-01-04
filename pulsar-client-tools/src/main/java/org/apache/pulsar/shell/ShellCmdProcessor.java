package org.apache.pulsar.shell;

import java.io.InputStream;
import java.io.PrintStream;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.felix.gogo.jline.Posix;
import org.apache.felix.gogo.jline.Procedural;
import org.apache.felix.gogo.jline.Shell;
import org.apache.felix.gogo.runtime.CommandProcessorImpl;
import org.apache.felix.gogo.runtime.CommandSessionImpl;
import org.jline.terminal.Terminal;

class ShellCmdProcessor extends CommandProcessorImpl {

    private static final String[] PROCEDURAL_FUNCTIONS = {
            "each", "if", "not", "throw", "try", "until", "while", "break", "continue"
    };

    static final String[] POSIX_FUNCTIONS = new String[]{
            "echo", "grep", "sleep", "pwd", "less", "head", "tail", "clear", "wc", "date",
    };


    private final Terminal terminal;

    public ShellCmdProcessor(Terminal terminal) {
        super(null);
        this.terminal = terminal;
        register(new Procedural(), PROCEDURAL_FUNCTIONS);
        register(new Posix(this), POSIX_FUNCTIONS);

    }

    public void register(Object target, String[] functions) {
        for (String function : functions) {
            register(target, function);
        }
    }

    public void register(Object target, String function) {
        addCommand("pulsar", target, function);
    }


    public boolean execute(CharSequence source) throws Exception {
        try (final CommandSessionImpl session = createSession(terminal.input(),
                terminal.output(),
                terminal.output())) {
            session.put(".FormatPipe", Boolean.FALSE);
            session.put(Shell.VAR_TERMINAL, terminal);
            final Object result = session.execute(source);
            if (result instanceof Boolean) {
                return (Boolean) result;
            }
        } catch (Throwable t) {
            if (ExceptionUtils.hasCause(t, org.apache.felix.gogo.runtime.CommandNotFoundException.class)) {
                System.out.println(t.getMessage());
                return false;
            }
            if (ExceptionUtils.hasCause(t, InterruptedException.class)) {
                // the cmd is executed in another thread. the interruption should not be propagated to the main thread.
                return false;
            }
            System.out.println("Got exception: " + t.getMessage());
            return false;
        }
        return true;
    }
}
