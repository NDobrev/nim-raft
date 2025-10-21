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
           "            width: 100%;\n" &
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
           "            width: 100%;\n" &
           "            background: #ffffff;\n" &
           "        }\n" &
           "\n" &
           "        .node-track {\n" &
           "            position: absolute;\n" &
           "            height: 60px;\n" &
           "            left: 0;\n" &
           "            right: 0;\n" &
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
           "            min-width: 100px; /* Ensure minimum width for timeline */\n" &
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
           "            z-index: 1;\n" &
           "        }\n" &
           "\n" &
           "        .candidate {\n" &
           "            background: #d97706;\n" &
           "            z-index: 1;\n" &
           "        }\n" &
           "\n" &
           "        .follower {\n" &
           "            background: #6b7280;\n" &
           "            z-index: 1;\n" &
           "        }\n" &
           "\n" &
           "        .commit {\n" &
            "            background: #2563eb;\n" &
            "            height: 20px;\n" &
            "            border-radius: 2px;\n" &
            "            z-index: 10;\n" &
            "        }\n" &
            "\n" &
            "        .replicated {\n" &
            "            background: #0ea5e9; /* sky-500 */\n" &
            "            height: 10px;\n" &
            "            border-radius: 2px;\n" &
            "            z-index: 11;\n" &
            "        }\n" &
           "\n" &
           "        .snapshot {\n" &
           "            background: #f59e0b; /* amber */\n" &
           "            height: 16px;\n" &
           "            border-radius: 50%;\n" &
           "            z-index: 12;\n" &
           "        }\n" &
           "\n" &
           "        .violation {\n" &
           "            background: #dc2626;\n" &
           "            height: 20px;\n" &
           "            border-radius: 2px;\n" &
           "            z-index: 10;\n" &
           "        }\n" &
           "\n" &
           "        .partition {\n" &
           "            background: #7c3aed;\n" &
           "            height: 20px;\n" &
           "            border-radius: 2px;\n" &
           "            z-index: 10;\n" &
           "        }\n" &
           "\n" &
           "        .restart {\n" &
           "            background: #db2777;\n" &
           "            height: 20px;\n" &
           "            border-radius: 2px;\n" &
           "            z-index: 10;\n" &
           "        }\n" &
           "\n" &
           "        .restart.wipe {\n" &
           "            background: #b91c1c; /* darker for wiped restarts */\n" &
           "            border: 2px solid #ffffff;\n" &
           "        }\n" &
           "\n" &
           "        .down {\n" &
           "            background: rgba(55, 65, 81, 0.25); /* semi-transparent gray */\n" &
           "            position: absolute;\n" &
           "            height: 40px;\n" &
           "            border-radius: 4px;\n" &
           "            z-index: 5;\n" &
           "        }\n" &
           "\n" &
           "        .committed {\n" &
           "            background: #ffb981;\n" &
           "            height: 16px;\n" &
           "            border-radius: 2px;\n" &
           "            z-index: 12;\n" &
           "            opacity: 0.9;\n" &
           "        }\n" &
           "        .committed.recommit {\n" &
           "            background: #f97316; /* darker orange for recommits */\n" &
           "            border: 1px solid #7c2d12;\n" &
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
           "                    <input type=\"range\" id=\"timeRange\" min=\"1\" max=\"" & $trace.totalTicks & "\" value=\"" & $trace.totalTicks & "\">\n" &
           "                    <span id=\"timeDisplay\">" & $trace.totalTicks & "ms</span>\n" &
           "                </div>\n" &
           "                <div class=\"control-group\">\n" &
           "                    <label for=\"showEvents\">Show Events:</label>\n" &
            "                    <input type=\"checkbox\" id=\"showCommits\" checked> Commits\n" &
            "                    <input type=\"checkbox\" id=\"showReplicated\" checked> Replicated\n" &
            "                    <input type=\"checkbox\" id=\"showCommitted\" checked> Committed\n" &
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
            "                <div class=\"legend-color\" style=\"background: #0ea5e9;\"></div>\n" &
            "                <span>Replicated (log tail advanced)</span>\n" &
            "            </div>\n" &
            "            <div class=\"legend-item\">\n" &
            "                <div class=\"legend-color\" style=\"background: #10b981;\"></div>\n" &
            "                <span>Committed</span>\n" &
            "            </div>\n" &
           "            <div class=\"legend-item\">\n" &
           "                <div class=\"legend-color\" style=\"background: #dc2626;\"></div>\n" &
           "                <span>Violations</span>\n" &
           "            </div>\n" &
           "            <div class=\"legend-item\">\n" &
           "                <div class=\"legend-color\" style=\"background: #7c3aed;\"></div>\n" &
           "                <span>Partitions</span>\n" &
           "            </div>\n" &
           "            <div class=\"legend-item\">\n" &
           "                <div class=\"legend-color\" style=\"background: rgba(55, 65, 81, 0.25);\"></div>\n" &
           "                <span>Down (node stopped)</span>\n" &
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
           "        const showCommits = document.getElementById('showCommits');\n" &
           "        const showReplicated = document.getElementById('showReplicated');\n" &
           "        const showCommitted = document.getElementById('showCommitted');\n" &
           "        const showViolations = document.getElementById('showViolations');\n" &
           "        const showPartitions = document.getElementById('showPartitions');\n" &
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
           "            // Get and sort node IDs lexicographically\n" &
           "            const nodeIds = [];\n" &
           "            for (let i = 0; i < nodeCount; i++) {\n" &
           "                nodeIds.push(traceData.clusterStates[0].nodes[i].id);\n" &
           "            }\n" &
           "            nodeIds.sort((a, b) => String(a).localeCompare(String(b)));\n" &
           "\n" &
           "            nodeIds.forEach((nodeId, trackIndex) => {\n" &
           "                const track = document.createElement('div');\n" &
           "                track.className = 'node-track';\n" &
           "                track.style.top = `${trackIndex * trackHeight}px`;\n" &
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
           "            });\n" &
           "\n" &
           "            // Draw time markers\n" &
           "            drawTimeMarkers();\n" &
           "        }\n" &
           "\n" &
           "        function drawNodeTimeline(nodeId, timeline) {\n" &
           "            // Handle edge case where currentMaxTime is 0\n" &
           "            if (currentMaxTime <= 0) {\n" &
           "                return;\n" &
           "            }\n" &
           "\n" &
           "            const states = traceData.clusterStates.filter(s => s.timeMs <= currentMaxTime);\n" &
           "            let lastRole = null;\n" &
           "            let lastTerm = 0;\n" &
           "            let lastTime = 0;\n" &
           "            let lastMarkedCommitIndex = 0;\n" &
           "            let lastMarkedTailIndex = -1;\n" &
           "            let lastAlive = true;\n" &
           "\n" &
           "            // Minimum width to ensure visibility (in pixels)\n" &
           "            const minWidthPx = 2;\n" &
           "            const canvas = document.getElementById('timelineCanvas');\n" &
           "            const canvasWidth = canvas.offsetWidth || Math.max(window.innerWidth - 100, 600);\n" &
           "            const timelineWidth = canvasWidth - 80; // subtract label width\n" &
           "            const minWidthPercent = (minWidthPx / timelineWidth) * 100;\n" &
           "\n" &
           "            // Draw role periods, skipping when node is down\n" &
           "            states.forEach((state, index) => {\n" &
           "                const node = state.nodes[state.nodes.findIndex(node => node.id === nodeId)];\n" &
           "                if (node) {\n" &
           "                    const isAlive = ('alive' in node) ? node.alive : true;\n" &
           "                    if (!isAlive) {\n" &
           "                        // If the node just went down, close any open role segment\n" &
           "                        if (lastRole !== null && lastAlive) {\n" &
           "                            const duration = state.timeMs - lastTime;\n" &
           "                            let width = (duration / currentMaxTime) * 100;\n" &
           "                            width = Math.max(width, minWidthPercent);\n" &
           "                            const left = (lastTime / currentMaxTime) * 100;\n" &
           "                            const roleDiv = document.createElement('div');\n" &
           "                            roleDiv.className = `event ${lastRole}`;\n" &
           "                            // Hue variation for leaders by term\n" &
           "                            if (lastRole === 'leader') {\n" &
           "                                const hue = (lastTerm % 12) * 15;\n" &
           "                                roleDiv.style.filter = `hue-rotate(${hue}deg)`;\n" &
           "                                const label = document.createElement('div');\n" &
           "                                label.style.position = 'absolute';\n" &
           "                                label.style.fontSize = '10px';\n" &
           "                                label.style.padding = '0 2px';\n" &
           "                                label.textContent = `t${lastTerm}`;\n" &
           "                                roleDiv.appendChild(label);\n" &
           "                            }\n" &
           "                            roleDiv.style.left = `${left}%`;\n" &
           "                            roleDiv.style.width = `${width}%`;\n" &
           "                            roleDiv.title = `${lastRole} (term ${lastTerm}) (${lastTime}-${state.timeMs}ms)`;\n" &
           "                            timeline.appendChild(roleDiv);\n" &
           "                        }\n" &
           "                        lastRole = null;\n" &
           "                        lastAlive = false;\n" &
           "                        lastTime = state.timeMs;\n" &
           "                    } else {\n" &
           "                        if (lastRole === null || !lastAlive) {\n" &
           "                            // Start a new role period\n" &
           "                            lastRole = node.role;\n" &
           "                            lastTerm = node.currentTerm;\n" &
           "                            // If we were down, start from this state's time\n" &
           "                            lastTime = state.timeMs;\n" &
           "                            lastAlive = true;\n" &
           "                        } else if (lastRole !== node.role || lastTerm !== node.currentTerm) {\n" &
           "                            // Role or term changed: close previous segment and start new one\n" &
           "                            const duration = state.timeMs - lastTime;\n" &
           "                            let width = (duration / currentMaxTime) * 100;\n" &
           "                            width = Math.max(width, minWidthPercent);\n" &
           "                            const left = (lastTime / currentMaxTime) * 100;\n" &
           "                            const roleDiv = document.createElement('div');\n" &
           "                            roleDiv.className = `event ${lastRole}`;\n" &
           "                            if (lastRole === 'leader') {\n" &
           "                                const hue = (lastTerm % 12) * 15;\n" &
           "                                roleDiv.style.filter = `hue-rotate(${hue}deg)`;\n" &
           "                                const label = document.createElement('div');\n" &
           "                                label.style.position = 'absolute';\n" &
           "                                label.style.fontSize = '10px';\n" &
           "                                label.style.padding = '0 2px';\n" &
           "                                label.textContent = `t${lastTerm}`;\n" &
           "                                roleDiv.appendChild(label);\n" &
           "                            }\n" &
           "                            roleDiv.style.left = `${left}%`;\n" &
           "                            roleDiv.style.width = `${width}%`;\n" &
           "                            roleDiv.title = `${lastRole} (term ${lastTerm}) (${lastTime}-${state.timeMs}ms)`;\n" &
           "                            timeline.appendChild(roleDiv);\n" &
           "                            lastRole = node.role;\n" &
           "                            lastTerm = node.currentTerm;\n" &
           "                            lastTime = state.timeMs;\n" &
           "                        }\n" &
           "                    }\n" &
           "\n" &
           "                    // Compute current real (non-monotonic) lastLogIndex from firstLogIndex/logLen\n" &
           "                    const realTail = (node.logLen > 0) ? (node.firstLogIndex + node.logLen - 1) : (node.firstLogIndex - 1);\n" &
           "\n" &
           "                    // Initialize or reset replication baseline if needed (e.g., after wipe)\n" &
           "                    if (lastMarkedTailIndex < 0) { lastMarkedTailIndex = realTail; }\n" &
           "                    if (realTail < lastMarkedTailIndex) { lastMarkedTailIndex = realTail; }\n" &
           "\n" &
           "                    // Draw replication advancement marker (log tail advanced)\n" &
           "                    if (isAlive && showReplicated.checked && realTail > lastMarkedTailIndex) {\n" &
           "                        const advanced = realTail - lastMarkedTailIndex;\n" &
           "                        const leftR = (state.timeMs / currentMaxTime) * 100;\n" &
           "                        const replDiv = document.createElement('div');\n" &
           "                        replDiv.className = 'event replicated';\n" &
           "                        replDiv.style.left = `${Math.max(leftR - 0.4, 0)}%`;\n" &
           "                        replDiv.style.width = `${Math.max(0.8, minWidthPercent)}%`;\n" &
           "                        replDiv.title = `lastLogIndex +${advanced} -> ${realTail} at ${state.timeMs}ms`;\n" &
           "                        timeline.appendChild(replDiv);\n" &
           "                        lastMarkedTailIndex = realTail;\n" &
           "                    }\n" &
           "\n" &
           "                    // Draw commit advancement marker (based on commitIndex delta)\n" &
           "                    // If commitIndex decreased (e.g., wiped restart), reset baseline to avoid bogus ranges\n" &
           "                    if (node.commitIndex < lastMarkedCommitIndex) { lastMarkedCommitIndex = node.commitIndex; }\n" &
           "                    if (isAlive && showCommits.checked && node.commitIndex > lastMarkedCommitIndex) {\n" &
           "                        const newCommits = node.commitIndex - lastMarkedCommitIndex;\n" &
           "                        const left = (state.timeMs / currentMaxTime) * 100;\n" &
           "                        const commitDiv = document.createElement('div');\n" &
           "                        commitDiv.className = 'event commit';\n" &
           "                        commitDiv.style.left = `${Math.max(left - 0.5, 0)}%`;\n" &
           "                        commitDiv.style.width = `${Math.max(1, minWidthPercent)}%`;\n" &
           "                        commitDiv.title = `commitIndex +${newCommits} -> ${node.commitIndex} at ${state.timeMs}ms`;\n" &
           "                        timeline.appendChild(commitDiv);\n" &
           "                        lastMarkedCommitIndex = node.commitIndex;\n" &
           "                    }\n" &
           "                }\n" &
           "            });\n" &
           "\n" &
           "            // Draw committed events for this node\n" &
           "            if (showCommitted.checked) {\n" &
           "                traceData.committedEvents.forEach(committedEvent => {\n" &
           "                    if (committedEvent.nodeId === nodeId && committedEvent.timeMs <= currentMaxTime) {\n" &
           "                        const left = (committedEvent.timeMs / currentMaxTime) * 100;\n" &
           "                        const committedDiv = document.createElement('div');\n" &
           "                        committedDiv.className = 'event committed' + (committedEvent.recommit ? ' recommit' : '');\n" &
           "                        committedDiv.style.left = `${Math.max(left - 0.3, 0)}%`;\n" &
           "                        committedDiv.style.width = `${Math.max(0.6, minWidthPercent)}%`;\n" &
           "                        committedDiv.title = `Entry ${committedEvent.entryIndex} (term ${committedEvent.entryTerm}) ${committedEvent.recommit ? 'recommitted' : 'committed'} at ${committedEvent.timeMs}ms`;\n" &
           "                        timeline.appendChild(committedDiv);\n" &
           "                    }\n" &
           "                });\n" &
           "            }\n" &
           "\n" &
           "            // Draw node down intervals and restart/snapshot markers using faultEvents\n" &
           "            const nodeFaults = (traceData.faultEvents || [])\n" &
           "                .filter(e => e.timeMs <= currentMaxTime && e.details && e.details.nodeId === nodeId && (e.faultType === 'node_stop' || e.faultType === 'node_restart' || e.faultType === 'snapshot'))\n" &
           "                .sort((a,b) => a.timeMs - b.timeMs);\n" &
           "            let downFrom = null;\n" &
           "            let wipeGeneration = 0; // increments on wiped restarts for this node\n" &
           "            nodeFaults.forEach(ev => {\n" &
           "                if (ev.faultType === 'node_stop' && downFrom === null) {\n" &
           "                    downFrom = ev.timeMs;\n" &
           "                } else if (ev.faultType === 'node_restart' && downFrom !== null) {\n" &
           "                    const start = downFrom;\n" &
           "                    const end = ev.timeMs;\n" &
           "                    const left = (start / currentMaxTime) * 100;\n" &
           "                    let width = ((end - start) / currentMaxTime) * 100;\n" &
           "                    width = Math.max(width, minWidthPercent);\n" &
           "                    const downDiv = document.createElement('div');\n" &
           "                    downDiv.className = 'down';\n" &
           "                    downDiv.style.left = `${left}%`;\n" &
           "                    downDiv.style.width = `${width}%`;\n" &
           "                    downDiv.title = `down (${start}-${end}ms)`;\n" &
           "                    timeline.appendChild(downDiv);\n" &
           "                    // Draw restart marker; highlight wipes\n" &
           "                    const restartDiv = document.createElement('div');\n" &
           "                    const wiped = !!(ev.details && ev.details.wipedDb);\n" &
           "                    if (wiped) { wipeGeneration += 1; }\n" &
           "                    restartDiv.className = 'event restart' + (wiped ? ' wipe' : '');\n" &
           "                    const rleft = (ev.timeMs / currentMaxTime) * 100;\n" &
           "                    restartDiv.style.left = `${Math.max(rleft - 0.3, 0)}%`;\n" &
           "                    restartDiv.style.width = `${Math.max(0.8, minWidthPercent)}%`;\n" &
           "                    restartDiv.title = `restart at ${ev.timeMs}ms (wipe: ${wiped}, gen: ${wipeGeneration})`;\n" &
           "                    timeline.appendChild(restartDiv);\n" &
           "                    downFrom = null;\n" &
           "                }\n" &
           "                // snapshot markers\n" &
           "                if (ev.faultType === 'snapshot') {\n" &
           "                    const left = (ev.timeMs / currentMaxTime) * 100;\n" &
           "                    const snapDiv = document.createElement('div');\n" &
           "                    snapDiv.className = 'event snapshot';\n" &
           "                    snapDiv.style.left = `${Math.max(left - 0.3, 0)}%`;\n" &
           "                    snapDiv.style.width = `${Math.max(0.8, minWidthPercent)}%`;\n" &
           "                    const idx = ev.details && ev.details.index;\n" &
           "                    const term = ev.details && ev.details.term;\n" &
           "                    snapDiv.title = `snapshot at ${ev.timeMs}ms (idx ${idx}, term ${term}, gen ${wipeGeneration})`;\n" &
           "                    timeline.appendChild(snapDiv);\n" &
           "                }\n" &
           "            });\n" &
           "            // If node is still down at currentMaxTime, draw till the end\n" &
           "            if (downFrom !== null) {\n" &
           "                const left = (downFrom / currentMaxTime) * 100;\n" &
           "                let width = ((currentMaxTime - downFrom) / currentMaxTime) * 100;\n" &
           "                width = Math.max(width, minWidthPercent);\n" &
           "                const downDiv = document.createElement('div');\n" &
           "                downDiv.className = 'down';\n" &
           "                downDiv.style.left = `${left}%`;\n" &
           "                downDiv.style.width = `${width}%`;\n" &
           "                downDiv.title = `down (${downFrom}-${currentMaxTime}ms)`;\n" &
           "                timeline.appendChild(downDiv);\n" &
           "            }\n" &
           "\n" &
           "            // Draw final role period (only if alive)\n" &
           "            if (lastRole && lastAlive) {\n" &
           "                const duration = currentMaxTime - lastTime;\n" &
           "                let width = (duration / currentMaxTime) * 100;\n" &
           "                // Ensure minimum width for visibility\n" &
           "                width = Math.max(width, minWidthPercent);\n" &
           "                const left = (lastTime / currentMaxTime) * 100;\n" &
           "\n" &
           "                const roleDiv = document.createElement('div');\n" &
           "                roleDiv.className = `event ${lastRole}`;\n" &
           "                if (lastRole === 'leader') {\n" &
           "                    const hue = (lastTerm % 12) * 15;\n" &
           "                    roleDiv.style.filter = `hue-rotate(${hue}deg)`;\n" &
           "                    const label = document.createElement('div');\n" &
           "                    label.style.position = 'absolute';\n" &
           "                    label.style.fontSize = '10px';\n" &
           "                    label.style.padding = '0 2px';\n" &
           "                    label.textContent = `t${lastTerm}`;\n" &
           "                    roleDiv.appendChild(label);\n" &
           "                }\n" &
           "                roleDiv.style.left = `${left}%`;\n" &
           "                roleDiv.style.width = `${width}%`;\n" &
           "                roleDiv.title = `${lastRole} (term ${lastTerm}) (${lastTime}-${currentMaxTime}ms)`;\n" &
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
           "            let allEvents = [\n" &
           "                ...traceData.rpcEvents.map(e => ({...e, type: 'rpc'})),\n" &
           "                ...traceData.faultEvents.map(e => ({...e, type: 'fault'})),\n" &
           "                ...traceData.committedEvents.map(e => ({...e, type: 'committed', entryIndex: e.entryIndex, entryTerm: e.entryTerm})),\n" &
           "                ...traceData.invariantViolations.map(e => ({...e, type: 'violation', description: e.description}))\n" &
           "            ].filter(e => e.timeMs <= currentMaxTime);\n" &
           "\n" &
           "            // Apply event type filters\n" &
           "            allEvents = allEvents.filter(e => {\n" &
           "                if (e.type === 'committed' && !showCommitted.checked) return false;\n" &
           "                if (e.type === 'violation' && !showViolations.checked) return false;\n" &
           "                if (e.type === 'fault' && !showPartitions.checked) return false;\n" &
           "                return true;\n" &
           "            });\n" &
           "\n" &
           "            allEvents.sort((a, b) => a.timeMs - b.timeMs);\n" &
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
           "                case 'committed':\n" &
           "                    return `Entry ${event.entryIndex} (term ${event.entryTerm}) committed by node ${event.nodeId}`;\n" &
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
           "        showCommits.addEventListener('change', function() {\n" &
           "            generateTimeline();\n" &
           "        });\n" &
           "\n" &
           "        showReplicated.addEventListener('change', function() {\n" &
           "            generateTimeline();\n" &
           "        });\n" &
           "\n" &
           "        showCommitted.addEventListener('change', function() {\n" &
           "            generateTimeline();\n" &
           "            generateEventsList();\n" &
           "        });\n" &
           "\n" &
           "        showViolations.addEventListener('change', function() {\n" &
           "            generateEventsList();\n" &
           "        });\n" &
           "\n" &
           "        showPartitions.addEventListener('change', function() {\n" &
           "            generateEventsList();\n" &
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
