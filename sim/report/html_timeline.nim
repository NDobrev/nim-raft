## HTML Timeline Generator - Creates interactive timeline visualizations
##
## Generates a self-contained HTML file showing simulation timeline with
## leaders, commits, partitions, restarts, and invariant violations.

import std/json
import std/strformat
import std/strutils
import std/options

import json_writer

type
  HtmlTimelineGenerator* = ref object
    trace*: JsonTrace

proc newHtmlTimelineGenerator*(trace: JsonTrace): HtmlTimelineGenerator =
  ## Create a new HTML timeline generator
  HtmlTimelineGenerator(trace: trace)

proc generateHtml*(generator: HtmlTimelineGenerator): string =
  ## Generate the complete HTML timeline as a string

  let trace = generator.trace

  # HTML template with embedded CSS and JavaScript
  result = "<!DOCTYPE html>\n" &
           "<html lang=\"en\">\n" &
           "<head>\n" &
           "    <meta charset=\"UTF-8\">\n" &
           "    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n" &
           "    <title>Raft Simulation Timeline - Seed " & $trace.seed & "</title>\n" &
           "    <style>\n" &
           "        body {\n" &
           "            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;\n" &
           "            margin: 0;\n" &
           "            padding: 20px;\n" &
           "            background: #f5f5f5;\n" &
           "        }\n" &
           "\n" &
           "        .header {\n" &
           "            background: white;\n" &
           "            padding: 20px;\n" &
           "            border-radius: 8px;\n" &
           "            box-shadow: 0 2px 4px rgba(0,0,0,0.1);\n" &
           "            margin-bottom: 20px;\n" &
           "        }\n" &
           "\n" &
           "        .summary {\n" &
           "            display: grid;\n" &
           "            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));\n" &
           "            gap: 20px;\n" &
           "            margin-bottom: 20px;\n" &
           "        }\n" &
           "\n" &
           "        .metric {\n" &
           "            background: white;\n" &
           "            padding: 15px;\n" &
           "            border-radius: 6px;\n" &
           "            box-shadow: 0 1px 3px rgba(0,0,0,0.1);\n" &
           "            text-align: center;\n" &
           "        }\n" &
           "\n" &
           "        .metric .value {\n" &
           "            font-size: 24px;\n" &
           "            font-weight: bold;\n" &
           "            color: #2563eb;\n" &
           "        }\n" &
           "\n" &
           "        .metric .label {\n" &
           "            font-size: 14px;\n" &
           "            color: #6b7280;\n" &
           "            margin-top: 5px;\n" &
           "        }\n" &
           "\n" &
           "        .timeline {\n" &
           "            background: white;\n" &
           "            border-radius: 8px;\n" &
           "            box-shadow: 0 2px 4px rgba(0,0,0,0.1);\n" &
           "            overflow: hidden;\n" &
           "        }\n" &
           "\n" &
           "        .timeline-header {\n" &
           "            padding: 15px 20px;\n" &
           "            background: #f8fafc;\n" &
           "            border-bottom: 1px solid #e2e8f0;\n" &
           "        }\n" &
           "\n" &
           "        .controls {\n" &
           "            display: flex;\n" &
           "            gap: 10px;\n" &
           "            align-items: center;\n" &
           "            margin-bottom: 15px;\n" &
           "        }\n" &
           "\n" &
           "        .control-group {\n" &
           "            display: flex;\n" &
           "            align-items: center;\n" &
           "            gap: 5px;\n" &
           "        }\n" &
           "\n" &
           "        .control-group label {\n" &
           "            font-size: 14px;\n" &
           "            font-weight: 500;\n" &
           "        }\n" &
           "\n" &
           "        input[type=\"range\"] {\n" &
           "            width: 100px;\n" &
           "        }\n" &
           "\n" &
           "        .timeline-canvas {\n" &
           "            position: relative;\n" &
           "            height: 400px;\n" &
           "            background: #ffffff;\n" &
           "        }\n" &
           "\n" &
           "        .node-track {\n" &
           "            position: absolute;\n" &
           "            height: 60px;\n" &
           "            border-bottom: 1px solid #e2e8f0;\n" &
           "            display: flex;\n" &
           "            align-items: center;\n" &
           "        }\n" &
           "\n" &
           "        .node-label {\n" &
           "            width: 80px;\n" &
           "            padding: 0 10px;\n" &
           "            font-size: 14px;\n" &
           "            font-weight: 500;\n" &
           "            color: #374151;\n" &
           "        }\n" &
           "\n" &
           "        .node-timeline {\n" &
           "            flex: 1;\n" &
           "            position: relative;\n" &
           "            height: 100%;\n" &
           "        }\n" &
           "\n" &
           "        .time-marker {\n" &
           "            position: absolute;\n" &
           "            top: 0;\n" &
           "            bottom: 0;\n" &
           "            width: 2px;\n" &
           "            background: #e2e8f0;\n" &
           "        }\n" &
           "\n" &
           "        .event {\n" &
           "            position: absolute;\n" &
           "            height: 40px;\n" &
           "            border-radius: 4px;\n" &
           "            display: flex;\n" &
           "            align-items: center;\n" &
           "            justify-content: center;\n" &
           "            font-size: 12px;\n" &
           "            font-weight: 500;\n" &
           "            color: white;\n" &
           "            cursor: pointer;\n" &
           "            transition: opacity 0.2s;\n" &
           "        }\n" &
           "\n" &
           "        .event:hover {\n" &
           "            opacity: 0.8;\n" &
           "        }\n" &
           "\n" &
           "        .leader {\n" &
           "            background: #059669;\n" &
           "        }\n" &
           "\n" &
           "        .candidate {\n" &
           "            background: #d97706;\n" &
           "        }\n" &
           "\n" &
           "        .follower {\n" &
           "            background: #6b7280;\n" &
           "        }\n" &
           "\n" &
           "        .commit {\n" &
           "            background: #2563eb;\n" &
           "            height: 20px;\n" &
           "            border-radius: 2px;\n" &
           "        }\n" &
           "\n" &
           "        .violation {\n" &
           "            background: #dc2626;\n" &
           "            height: 20px;\n" &
           "            border-radius: 2px;\n" &
           "        }\n" &
           "\n" &
           "        .partition {\n" &
           "            background: #7c3aed;\n" &
           "            height: 20px;\n" &
           "            border-radius: 2px;\n" &
           "        }\n" &
           "\n" &
           "        .restart {\n" &
           "            background: #db2777;\n" &
           "            height: 20px;\n" &
           "            border-radius: 2px;\n" &
           "        }\n" &
           "\n" &
           "        .legend {\n" &
           "            display: flex;\n" &
           "            flex-wrap: wrap;\n" &
           "            gap: 20px;\n" &
           "            padding: 15px 20px;\n" &
           "            background: #f8fafc;\n" &
           "            border-top: 1px solid #e2e8f0;\n" &
           "        }\n" &
           "\n" &
           "        .legend-item {\n" &
           "            display: flex;\n" &
           "            align-items: center;\n" &
           "            gap: 5px;\n" &
           "            font-size: 14px;\n" &
           "        }\n" &
           "\n" &
           "        .legend-color {\n" &
           "            width: 16px;\n" &
           "            height: 16px;\n" &
           "            border-radius: 3px;\n" &
           "        }\n" &
           "\n" &
           "        .events-panel {\n" &
           "            background: white;\n" &
           "            border-radius: 8px;\n" &
           "            box-shadow: 0 2px 4px rgba(0,0,0,0.1);\n" &
           "            margin-top: 20px;\n" &
           "            overflow: hidden;\n" &
           "        }\n" &
           "\n" &
           "        .events-header {\n" &
           "            padding: 15px 20px;\n" &
           "            background: #f8fafc;\n" &
           "            border-bottom: 1px solid #e2e8f0;\n" &
           "            font-weight: 600;\n" &
           "        }\n" &
           "\n" &
           "        .events-list {\n" &
           "            max-height: 300px;\n" &
           "            overflow-y: auto;\n" &
           "        }\n" &
           "\n" &
           "        .event-item {\n" &
           "            padding: 10px 20px;\n" &
           "            border-bottom: 1px solid #f1f5f9;\n" &
           "            cursor: pointer;\n" &
           "            transition: background 0.2s;\n" &
           "        }\n" &
           "\n" &
           "        .event-item:hover {\n" &
           "            background: #f8fafc;\n" &
           "        }\n" &
           "\n" &
           "        .event-time {\n" &
           "            font-weight: 500;\n" &
           "            color: #2563eb;\n" &
           "        }\n" &
           "\n" &
           "        .event-type {\n" &
           "            font-size: 12px;\n" &
           "            color: #6b7280;\n" &
           "            text-transform: uppercase;\n" &
           "            letter-spacing: 0.5px;\n" &
           "        }\n" &
           "\n" &
           "        .event-desc {\n" &
           "            margin-top: 2px;\n" &
           "            color: #374151;\n" &
           "        }\n" &
           "    </style>\n" &
           "</head>\n" &
           "<body>\n" &
           "    <div class=\"header\">\n" &
           "        <h1>Raft Simulation Timeline</h1>\n" &
           "        <div class=\"summary\">\n" &
           "            <div class=\"metric\">\n" &
           "                <div class=\"value\">" & $trace.totalTicks & "ms</div>\n" &
           "                <div class=\"label\">Total Time</div>\n" &
           "            </div>\n" &
           "            <div class=\"metric\">\n" &
           "                <div class=\"value\">" & $trace.messageStats.totalMessages & "</div>\n" &
           "                <div class=\"label\">Total Messages</div>\n" &
           "            </div>\n" &
           "            <div class=\"metric\">\n" &
           "                <div class=\"value\">" & $trace.messageStats.networkStats["delivered"].getInt() & "</div>\n" &
           "                <div class=\"label\">Delivered</div>\n" &
           "            </div>\n" &
           "            <div class=\"metric\">\n" &
           "                <div class=\"value\">" & $trace.messageStats.networkStats["dropped"].getInt() & "</div>\n" &
           "                <div class=\"label\">Dropped</div>\n" &
           "            </div>\n" &
           "            <div class=\"metric\">\n" &
           "                <div class=\"value\">" & $trace.rpcEvents.len & "</div>\n" &
           "                <div class=\"label\">RPC Events</div>\n" &
           "            </div>\n" &
           "            <div class=\"metric\">\n" &
           "                <div class=\"value\">" & $trace.invariantViolations.len & "</div>\n" &
           "                <div class=\"label\">Violations</div>\n" &
           "            </div>\n" &
           "            <div class=\"metric\">\n" &
           "                <div class=\"value\">" & (if trace.success: "✓" else: "✗") & "</div>\n" &
           "                <div class=\"label\">Success</div>\n" &
           "            </div>\n" &
           "        </div>\n" &
           "    </div>\n" &
           "\n" &
           "    <div class=\"timeline\">\n" &
           "        <div class=\"timeline-header\">\n" &
           "            <div class=\"controls\">\n" &
           "                <div class=\"control-group\">\n" &
           "                    <label for=\"timeRange\">Time Range:</label>\n" &
           "                    <input type=\"range\" id=\"timeRange\" min=\"0\" max=\"" & $trace.totalTicks & "\" value=\"" & $trace.totalTicks & "\">\n" &
           "                    <span id=\"timeDisplay\">" & $trace.totalTicks & "ms</span>\n" &
           "                </div>\n" &
           "                <div class=\"control-group\">\n" &
           "                    <label for=\"showEvents\">Show Events:</label>\n" &
           "                    <input type=\"checkbox\" id=\"showCommits\" checked> Commits\n" &
           "                    <input type=\"checkbox\" id=\"showViolations\" checked> Violations\n" &
           "                    <input type=\"checkbox\" id=\"showPartitions\" checked> Partitions\n" &
           "                </div>\n" &
           "            </div>\n" &
           "        </div>\n" &
           "\n" &
           "        <div class=\"timeline-canvas\" id=\"timelineCanvas\">\n" &
           "            <!-- Timeline content will be generated by JavaScript -->\n" &
           "        </div>\n" &
           "\n" &
           "        <div class=\"legend\">\n" &
           "            <div class=\"legend-item\">\n" &
           "                <div class=\"legend-color\" style=\"background: #059669;\"></div>\n" &
           "                <span>Leader</span>\n" &
           "            </div>\n" &
           "            <div class=\"legend-item\">\n" &
           "                <div class=\"legend-color\" style=\"background: #d97706;\"></div>\n" &
           "                <span>Candidate</span>\n" &
           "            </div>\n" &
           "            <div class=\"legend-item\">\n" &
           "                <div class=\"legend-color\" style=\"background: #6b7280;\"></div>\n" &
           "                <span>Follower</span>\n" &
           "            </div>\n" &
           "            <div class=\"legend-item\">\n" &
           "                <div class=\"legend-color\" style=\"background: #2563eb;\"></div>\n" &
           "                <span>Commits</span>\n" &
           "            </div>\n" &
           "            <div class=\"legend-item\">\n" &
           "                <div class=\"legend-color\" style=\"background: #dc2626;\"></div>\n" &
           "                <span>Violations</span>\n" &
           "            </div>\n" &
           "            <div class=\"legend-item\">\n" &
           "                <div class=\"legend-color\" style=\"background: #7c3aed;\"></div>\n" &
           "                <span>Partitions</span>\n" &
           "            </div>\n" &
           "        </div>\n" &
           "    </div>\n" &
           "\n" &
           "    <div class=\"events-panel\">\n" &
           "        <div class=\"events-header\">Events Timeline</div>\n" &
           "        <div class=\"events-list\" id=\"eventsList\">\n" &
           "            <!-- Events will be populated by JavaScript -->\n" &
           "        </div>\n" &
           "    </div>\n" &
           "\n" &
           "    <div class=\"events-panel\">\n" &
           "        <div class=\"events-header\">Message Statistics</div>\n" &
           "        <div class=\"events-list\" id=\"messageStatsList\">\n" &
           "            <!-- Message stats will be populated by JavaScript -->\n" &
           "        </div>\n" &
           "    </div>\n" &
           "\n" &
           "    <script>\n" &
           "        // Simulation data\n" &
           "        const traceData = " & $(%trace) & ";\n" &
           "\n" &
           "        // DOM elements\n" &
           "        const canvas = document.getElementById('timelineCanvas');\n" &
           "        const eventsList = document.getElementById('eventsList');\n" &
           "        const messageStatsList = document.getElementById('messageStatsList');\n" &
           "        const timeRange = document.getElementById('timeRange');\n" &
           "        const timeDisplay = document.getElementById('timeDisplay');\n" &
           "\n" &
           "        let maxTime = traceData.totalTicks;\n" &
           "        let currentMaxTime = maxTime;\n" &
           "\n" &
           "        // Generate timeline visualization\n" &
           "        function generateTimeline() {\n" &
           "            canvas.innerHTML = '';\n" &
           "\n" &
           "            // Calculate node tracks\n" &
           "            const nodeCount = traceData.config.cluster.nodes;\n" &
           "            const trackHeight = 60;\n" &
           "\n" &
           "            for (let nodeId = 0; nodeId < nodeCount; nodeId++) {\n" &
           "                const track = document.createElement('div');\n" &
           "                track.className = 'node-track';\n" &
           "                track.style.top = `${nodeId * trackHeight}px`;\n" &
           "\n" &
           "                const label = document.createElement('div');\n" &
           "                label.className = 'node-label';\n" &
           "                label.textContent = `Node ${nodeId}`;\n" &
           "                track.appendChild(label);\n" &
           "\n" &
           "                const timeline = document.createElement('div');\n" &
           "                timeline.className = 'node-timeline';\n" &
           "                track.appendChild(timeline);\n" &
           "\n" &
           "                canvas.appendChild(track);\n" &
           "\n" &
           "                // Draw role changes and events\n" &
           "                drawNodeTimeline(nodeId, timeline);\n" &
           "            }\n" &
           "\n" &
           "            // Draw time markers\n" &
           "            drawTimeMarkers();\n" &
           "        }\n" &
           "\n" &
           "        function drawNodeTimeline(nodeId, timeline) {\n" &
           "            const states = traceData.clusterStates.filter(s => s.timeMs <= currentMaxTime);\n" &
           "            let lastRole = null;\n" &
           "            let lastTime = 0;\n" &
           "\n" &
           "            // Draw role periods\n" &
           "            states.forEach(state => {\n" &
           "                const node = state.nodes[nodeId];\n" &
           "                if (node) {\n" &
           "                    if (lastRole !== node.role) {\n" &
           "                        if (lastRole) {\n" &
           "                            // End previous role period\n" &
           "                            const duration = state.timeMs - lastTime;\n" &
           "                            const width = (duration / currentMaxTime) * 100;\n" &
           "                            const left = (lastTime / currentMaxTime) * 100;\n" &
           "\n" &
           "                            const roleDiv = document.createElement('div');\n" &
           "                            roleDiv.className = `event ${lastRole}`;\n" &
           "                            roleDiv.style.left = `${left}%`;\n" &
           "                            roleDiv.style.width = `${width}%`;\n" &
           "                            roleDiv.title = `${lastRole} (${lastTime}-${state.timeMs}ms)`;\n" &
           "                            timeline.appendChild(roleDiv);\n" &
           "                        }\n" &
           "                        lastRole = node.role;\n" &
           "                        lastTime = state.timeMs;\n" &
           "                    }\n" &
           "\n" &
           "                    // Draw commit events\n" &
           "                    if (node.commitIndex > 0) {\n" &
           "                        const left = (state.timeMs / currentMaxTime) * 100;\n" &
           "                        const commitDiv = document.createElement('div');\n" &
           "                        commitDiv.className = 'event commit';\n" &
           "                        commitDiv.style.left = `${left - 0.5}%`;\n" &
           "                        commitDiv.style.width = '1%';\n" &
           "                        commitDiv.title = `Commit ${node.commitIndex} at ${state.timeMs}ms`;\n" &
           "                        timeline.appendChild(commitDiv);\n" &
           "                    }\n" &
           "                }\n" &
           "            });\n" &
           "\n" &
           "            // Draw final role period\n" &
           "            if (lastRole) {\n" &
           "                const duration = currentMaxTime - lastTime;\n" &
           "                const width = (duration / currentMaxTime) * 100;\n" &
           "                const left = (lastTime / currentMaxTime) * 100;\n" &
           "\n" &
           "                const roleDiv = document.createElement('div');\n" &
           "                roleDiv.className = `event ${lastRole}`;\n" &
           "                roleDiv.style.left = `${left}%`;\n" &
           "                roleDiv.style.width = `${width}%`;\n" &
           "                roleDiv.title = `${lastRole} (${lastTime}-${currentMaxTime}ms)`;\n" &
           "                timeline.appendChild(roleDiv);\n" &
           "            }\n" &
           "        }\n" &
           "\n" &
           "        function drawTimeMarkers() {\n" &
           "            const markers = [0, currentMaxTime * 0.25, currentMaxTime * 0.5, currentMaxTime * 0.75, currentMaxTime];\n" &
           "            markers.forEach(time => {\n" &
           "                const marker = document.createElement('div');\n" &
           "                marker.className = 'time-marker';\n" &
           "                marker.style.left = `${(time / currentMaxTime) * 100}%`;\n" &
           "\n" &
           "                const label = document.createElement('div');\n" &
           "                label.style.position = 'absolute';\n" &
           "                label.style.top = '-20px';\n" &
           "                label.style.left = '5px';\n" &
           "                label.style.fontSize = '12px';\n" &
           "                label.style.color = '#6b7280';\n" &
           "                label.textContent = `${time}ms`;\n" &
           "                marker.appendChild(label);\n" &
           "\n" &
           "                canvas.appendChild(marker);\n" &
           "            });\n" &
           "        }\n" &
           "\n" &
           "        function generateEventsList() {\n" &
           "            eventsList.innerHTML = '';\n" &
           "\n" &
           "            // Combine all events\n" &
           "            const allEvents = [\n" &
           "                ...traceData.rpcEvents.map(e => ({...e, type: 'rpc'})),\n" &
           "                ...traceData.faultEvents.map(e => ({...e, type: 'fault'})),\n" &
           "                ...traceData.invariantViolations.map(e => ({...e, type: 'violation', description: e.description}))\n" &
           "            ].filter(e => e.timeMs <= currentMaxTime)\n" &
           "            .sort((a, b) => a.timeMs - b.timeMs);\n" &
           "\n" &
           "            allEvents.forEach(event => {\n" &
           "                const item = document.createElement('div');\n" &
           "                item.className = 'event-item';\n" &
           "\n" &
           "                item.innerHTML = `\n" &
           "                    <div class=\"event-time\">${event.timeMs}ms</div>\n" &
           "                    <div class=\"event-type\">${event.type}</div>\n" &
           "                    <div class=\"event-desc\">${getEventDescription(event)}</div>\n" &
           "                `;\n" &
           "\n" &
           "                eventsList.appendChild(item);\n" &
           "            });\n" &
           "        }\n" &
           "\n" &
           "        function getEventDescription(event) {\n" &
           "            switch (event.type) {\n" &
           "                case 'rpc':\n" &
           "                    return `${event.rpcType} from ${(event.fromNode || 0)} to ${(event.toNode || 0)}`;\n" &
           "                case 'fault':\n" &
           "                    return event.faultType;\n" &
           "                case 'violation':\n" &
           "                    return event.description;\n" &
           "                default:\n" &
           "                    return 'Unknown event';\n" &
           "            }\n" &
           "        }\n" &
           "\n" &
           "        function generateMessageStatsList() {\n" &
           "            messageStatsList.innerHTML = '';\n" &
           "\n" &
           "            // Message type statistics\n" &
           "            const messageStats = traceData.messageStats;\n" &
           "            const totalMessages = messageStats.totalMessages;\n" &
           "\n" &
           "            // Network stats\n" &
           "            const networkStats = messageStats.networkStats;\n" &
           "            const item1 = document.createElement('div');\n" &
           "            item1.className = 'event-item';\n" &
           "            item1.innerHTML = `\n" &
           "                <div class=\"event-type\">Network</div>\n" &
           "                <div class=\"event-desc\">Delivered: ${networkStats.delivered || 0}, Dropped: ${networkStats.dropped || 0}, Duplicated: ${networkStats.duplicated || 0}</div>\n" &
           "            `;\n" &
           "            messageStatsList.appendChild(item1);\n" &
           "\n" &
           "            // Message type breakdown\n" &
           "            if (messageStats.messagesByType) {\n" &
           "                Object.entries(messageStats.messagesByType).forEach(([type, count]) => {\n" &
           "                    const percentage = totalMessages > 0 ? ((count / totalMessages) * 100).toFixed(1) : '0.0';\n" &
           "                    const item = document.createElement('div');\n" &
           "                    item.className = 'event-item';\n" &
           "                    item.innerHTML = `\n" &
           "                        <div class=\"event-type\">${type}</div>\n" &
           "                        <div class=\"event-desc\">${count} messages (${percentage}%)</div>\n" &
           "                    `;\n" &
           "                    messageStatsList.appendChild(item);\n" &
           "                });\n" &
           "            }\n" &
           "        }\n" &
           "\n" &
           "        // Event handlers\n" &
           "        timeRange.addEventListener('input', function() {\n" &
           "            currentMaxTime = parseInt(this.value);\n" &
           "            timeDisplay.textContent = currentMaxTime + 'ms';\n" &
           "            generateTimeline();\n" &
           "            generateEventsList();\n" &
           "            generateMessageStatsList();\n" &
           "        });\n" &
           "\n" &
           "        // Initialize\n" &
           "        generateTimeline();\n" &
           "        generateEventsList();\n" &
           "        generateMessageStatsList();\n" &
           "    </script>\n" &
           "</body>\n" &
           "</html>\n"

proc writeToFile*(generator: HtmlTimelineGenerator, path: string) =
  ## Write the HTML timeline to a file
  let html = generator.generateHtml()
  writeFile(path, html)

proc `$`*(generator: HtmlTimelineGenerator): string =
  ## String representation
  fmt"HtmlTimelineGenerator(states={generator.trace.clusterStates.len}, events={generator.trace.rpcEvents.len + generator.trace.faultEvents.len})"
