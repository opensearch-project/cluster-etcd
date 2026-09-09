package org.opensearch.supervisor.app;

/**
 * Entry point. Milestone 1 is the skeleton: this exists so the module graph is real and the application
 * plugin has something to point at. The loop arrives in milestone 3, and it will not act until milestone 5.
 */
public final class Main {

    private Main() {}

    public static void main(String[] args) {
        System.out.println("opensearch-supervisor 0.1.0-SNAPSHOT — skeleton only, no reconciliation loop yet.");
        System.out.println("See DESIGN.md §11 for what lands when.");
    }
}
