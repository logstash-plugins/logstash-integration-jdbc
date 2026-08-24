# encoding: utf-8

# Shared fixture for the security_statements integration test table.
#
# Mirrors the data shape and seeded RNG from
# reproducer_198_OOM_on_prepared_statement/fill_db.java so that tests
# reproduce the original OOM scenario faithfully.
module SecurityStatementsFixture
  BATCH_SIZE = 500

  CVE_POOL = [
    { cve_id: "CVE-2021-44228", score: 10.0,
      title: "Log4Shell – Apache Log4j2 Remote Code Execution",
      description: "Apache Log4j2 2.0-beta9 through 2.14.1 JNDI features used in configuration, log messages, and parameters do not protect against attacker controlled LDAP and other JNDI related endpoints. An attacker who can control log messages or log message parameters can execute arbitrary code loaded from LDAP servers when message lookup substitution is enabled.",
      affected_products: "Apache Log4j2 2.0-beta9 through 2.14.1; all applications embedding Log4j2 in those versions including VMware vCenter, Cisco products, Elastic Stack, Minecraft Java Edition, and thousands of enterprise products.",
      remediation: "Upgrade to Apache Log4j 2.15.0 or later. If immediate upgrade is not possible, set the system property 'log4j2.formatMsgNoLookups' to 'true' or remove the JndiLookup class from the classpath. Apply vendor-specific patches as released.",
      references: "https://nvd.nist.gov/vuln/detail/CVE-2021-44228, https://logging.apache.org/log4j/2.x/security.html, https://www.cisa.gov/known-exploited-vulnerabilities-catalog",
      statement: "CRITICAL severity. Actively exploited in the wild since December 2021. CISA added to KEV catalog. Affects a vast number of Java-based enterprise products. Immediate patching required.",
      reporter: "NVD / Apache Security Team" },

    { cve_id: "CVE-2014-0160", score: 7.5,
      title: "Heartbleed – OpenSSL TLS Heartbeat Information Disclosure",
      description: "The TLS and DTLS implementations in OpenSSL 1.0.1 before 1.0.1g do not properly handle Heartbeat Extension packets, which allows remote attackers to obtain sensitive information from process memory via crafted packets that trigger a buffer over-read, as demonstrated by reading private keys.",
      affected_products: "OpenSSL 1.0.1 through 1.0.1f; products using affected OpenSSL versions including nginx, Apache httpd, OpenVPN, and numerous VPN appliances and operating-system distributions.",
      remediation: "Upgrade to OpenSSL 1.0.1g or later. Revoke and reissue all TLS certificates generated with vulnerable versions. Force password resets for services exposed during the vulnerable window. Enable Perfect Forward Secrecy.",
      references: "https://nvd.nist.gov/vuln/detail/CVE-2014-0160, https://heartbleed.com, https://www.openssl.org/news/secadv/20140407.txt",
      statement: "HIGH severity. Allows passive exfiltration of private keys, session tokens, and credentials without leaving traces in server logs. Widely exploited. Certificates signed with compromised keys must be reissued.",
      reporter: "NVD / Codenomicon / Google Security" },

    { cve_id: "CVE-2017-0144", score: 8.1,
      title: "EternalBlue – Windows SMBv1 Remote Code Execution",
      description: "The SMBv1 server in Microsoft Windows Vista SP2, Windows Server 2008 SP2 and R2 SP1, Windows 7 SP1, Windows 8.1, Windows Server 2012 Gold and R2, Windows RT 8.1, and Windows 10 Gold, 1511, and 1600, and Windows Server 2016 allows remote attackers to execute arbitrary code via crafted packets.",
      affected_products: "Microsoft Windows Vista, 7, 8.1, 10, Server 2008, 2012, 2016 with SMBv1 enabled.",
      remediation: "Apply Microsoft Security Bulletin MS17-010. Disable SMBv1 protocol. Block inbound SMB traffic (TCP port 445) at the network perimeter. Deploy Windows Defender Credential Guard where applicable.",
      references: "https://nvd.nist.gov/vuln/detail/CVE-2017-0144, https://technet.microsoft.com/en-us/library/security/ms17-010.aspx",
      statement: "HIGH severity. Exploited by WannaCry and NotPetya ransomware campaigns causing billions in damages. NSA exploit leaked by Shadow Brokers. Disable SMBv1 immediately even if patch cannot be applied.",
      reporter: "NVD / Microsoft MSRC" },

    { cve_id: "CVE-2014-6271", score: 9.8,
      title: "ShellShock – GNU Bash Environment Variable RCE",
      description: "GNU Bash through 4.3 processes trailing strings after function definitions in the values of environment variables, which allows remote attackers to execute arbitrary code via a crafted environment, as demonstrated through vectors involving the ForceCommand feature in OpenSSH, the mod_cgi and mod_cgid modules in Apache HTTP Server, scripts executed by unspecified DHCP clients, and other situations in which setting the environment occurs across a privilege boundary from Bash execution.",
      affected_products: "GNU Bash versions through 4.3; CGI scripts on Apache/nginx, SSH ForceCommand setups, DHCP client hooks, Docker containers with Bash entrypoints.",
      remediation: "Upgrade GNU Bash to version 4.3 patch 25 or later. Apply vendor OS patches. Audit all CGI scripts and shell-invocation paths. Prefer non-shell interpreters for network-facing scripts.",
      references: "https://nvd.nist.gov/vuln/detail/CVE-2014-6271, https://www.gnu.org/software/bash/",
      statement: "CRITICAL severity. Trivial to exploit remotely via HTTP headers, DHCP options, or SSH. Worm-like propagation observed within hours of disclosure. Patch all Bash installations immediately.",
      reporter: "NVD / Stephane Chazelas / Red Hat Security" },

    { cve_id: "CVE-2022-22965", score: 9.8,
      title: "Spring4Shell – Spring Framework RCE via Data Binding",
      description: "A Spring MVC or Spring WebFlux application running on JDK 9+ may be vulnerable to remote code execution via data binding. The specific exploit requires the application to run on Tomcat as a WAR deployment. If the application is deployed as a Spring Boot executable jar (default), it is not vulnerable to the exploit.",
      affected_products: "Spring Framework 5.3.0 to 5.3.17, 5.2.0 to 5.2.19 and older versions; applications deployed as WAR on Apache Tomcat running JDK 9 or higher.",
      remediation: "Upgrade to Spring Framework 5.3.18+ or 5.2.20+. For Spring Boot users, upgrade to 2.6.6 or 2.5.12. Alternatively, add @InitBinder to disallow binding of class and classLoader fields.",
      references: "https://nvd.nist.gov/vuln/detail/CVE-2022-22965, https://spring.io/blog/2022/03/31/spring-framework-rce-early-announcement",
      statement: "CRITICAL severity. Unauthenticated RCE in a widely used Java framework. PoC exploit publicly available. Assess WAR deployments on Tomcat + JDK9+ as the highest priority.",
      reporter: "NVD / Spring Security Team" },

    { cve_id: "CVE-2019-0708", score: 9.8,
      title: "BlueKeep – Windows Remote Desktop Services Pre-Auth RCE",
      description: "A remote code execution vulnerability exists in Remote Desktop Services when an unauthenticated attacker connects to the target system using RDP and sends specially crafted requests. This vulnerability is pre-authentication and requires no user interaction.",
      affected_products: "Windows XP, Windows 7, Windows Server 2003, Windows Server 2008 and R2 with RDP exposed.",
      remediation: "Apply Microsoft patch KB4499175 (Windows 7) or equivalent. Enable Network Level Authentication. Block TCP port 3389 at the network perimeter. Consider disabling RDP if not required.",
      references: "https://nvd.nist.gov/vuln/detail/CVE-2019-0708, https://portal.msrc.microsoft.com/en-US/security-guidance/advisory/CVE-2019-0708",
      statement: "CRITICAL severity. Wormable vulnerability similar in character to MS17-010. NSA publicly urged patching. Metasploit module available. Prioritize internet-exposed RDP systems.",
      reporter: "NVD / Microsoft MSRC" },

    { cve_id: "CVE-2021-26855", score: 9.1,
      title: "ProxyLogon – Microsoft Exchange Server SSRF",
      description: "Microsoft Exchange Server is vulnerable to a server-side request forgery (SSRF) vulnerability that allows attackers to send arbitrary HTTP requests and authenticate as the Exchange server, bypassing authentication. Used as the initial vector in the ProxyLogon exploit chain.",
      affected_products: "Microsoft Exchange Server 2013 CU23, Exchange Server 2016 CU18/CU19, Exchange Server 2019 CU7/CU8.",
      remediation: "Install Microsoft Security Update KB5000871 immediately. If patching is impossible, run Microsoft's mitigation script. Audit Exchange IIS logs for indicators of compromise (IOCs) provided by Microsoft.",
      references: "https://nvd.nist.gov/vuln/detail/CVE-2021-26855, https://msrc.microsoft.com/update-guide/vulnerability/CVE-2021-26855, https://www.microsoft.com/security/blog/2021/03/02/hafnium-targeting-exchange-servers/",
      statement: "CRITICAL severity. Chained with CVE-2021-27065 for post-auth RCE, enabling webshell deployment. Attributed to HAFNIUM threat group. Tens of thousands of Exchange servers compromised. Emergency patch required.",
      reporter: "NVD / Microsoft MSRC / Volexity" },

    { cve_id: "CVE-2021-34527", score: 8.8,
      title: "PrintNightmare – Windows Print Spooler Privilege Escalation / RCE",
      description: "Windows Print Spooler Remote Code Execution Vulnerability. The Windows Print Spooler service improperly performs privileged file operations. An attacker who successfully exploits this vulnerability could run arbitrary code with SYSTEM privileges. Attack vectors include both remote (via SMB) and local.",
      affected_products: "All supported Windows versions with Print Spooler service running (enabled by default).",
      remediation: "Apply Microsoft cumulative updates released July 2021. As an interim measure, disable the Print Spooler service on domain controllers and systems that do not need printing. Restrict inbound SMB traffic.",
      references: "https://nvd.nist.gov/vuln/detail/CVE-2021-34527, https://msrc.microsoft.com/update-guide/vulnerability/CVE-2021-34527",
      statement: "HIGH severity. PoC published on GitHub before patch availability. Widely used by ransomware operators for lateral movement and privilege escalation. Disable Print Spooler on DCs immediately.",
      reporter: "NVD / Microsoft MSRC" },

    { cve_id: "CVE-2022-30190", score: 7.8,
      title: "Follina – Microsoft Support Diagnostic Tool RCE",
      description: "A remote code execution vulnerability exists when MSDT is called using the URL protocol from a calling application such as Word. An attacker who successfully exploits this vulnerability can run arbitrary code with the privileges of the calling application.",
      affected_products: "Windows 7 through 11, Windows Server 2008 through 2022 with Microsoft Support Diagnostic Tool (MSDT).",
      remediation: "Apply June 2022 Patch Tuesday updates. As interim mitigation, disable MSDT URL protocol by deleting or renaming the HKEY_CLASSES_ROOT\\ms-msdt registry key. Configure Attack Surface Reduction rules.",
      references: "https://nvd.nist.gov/vuln/detail/CVE-2022-30190, https://msrc.microsoft.com/update-guide/vulnerability/CVE-2022-30190",
      statement: "HIGH severity. Exploited via malicious Office documents and RTF files without requiring macros. No user interaction beyond opening the document. Observed in campaigns by TA570 and state-sponsored actors.",
      reporter: "NVD / Microsoft MSRC / nao_sec" },

    { cve_id: "CVE-2016-5195", score: 7.8,
      title: "Dirty COW – Linux Kernel Privilege Escalation",
      description: "Race condition in mm/gup.c in the Linux kernel before 4.8.3 allows local users to gain privileges by leveraging incorrect handling of a copy-on-write (COW) feature to write to a read-only memory mapping.",
      affected_products: "Linux kernel 2.x through 4.x before 4.8.3; Android devices running affected kernel versions; all major Linux distributions prior to vendor patches.",
      remediation: "Upgrade to Linux kernel 4.8.3 or later. Apply distribution-specific patches (RHEL, Ubuntu, Debian, CentOS). For Android devices, apply vendor-specific security patches. Reboot required after patching.",
      references: "https://nvd.nist.gov/vuln/detail/CVE-2016-5195, https://dirtycow.ninja/",
      statement: "HIGH severity. Nine-year-old vulnerability in the Linux kernel. Reliable public exploits for multiple architectures. Actively exploited in Android malware. Patch and reboot all Linux systems.",
      reporter: "NVD / Phil Oester / Linux Kernel Security Team" },

    { cve_id: "CVE-2017-5753", score: 5.6,
      title: "Spectre Variant 1 – Bounds Check Bypass",
      description: "Systems with microprocessors utilizing speculative execution and branch prediction may allow unauthorized disclosure of information to an attacker with local user access via a side-channel analysis of the data cache. Affects Intel, AMD, and ARM processors.",
      affected_products: "All modern CPUs with speculative execution: Intel Core (generations 2+), AMD Ryzen/EPYC, ARM Cortex-A series; cloud hypervisors exposing shared CPU resources.",
      remediation: "Apply OS and hypervisor patches for Spectre mitigations (KPTI, Retpoline). Update browser engines to reduce timer resolution and disable SharedArrayBuffer. Apply CPU microcode updates where available.",
      references: "https://nvd.nist.gov/vuln/detail/CVE-2017-5753, https://spectreattack.com/",
      statement: "MEDIUM severity (per NVD). Fundamental CPU architectural vulnerability with no full software fix. Performance overhead from mitigations is workload-dependent (2-30%). Ongoing mitigation strategy required.",
      reporter: "NVD / Google Project Zero / Graz University of Technology" },

    { cve_id: "CVE-2017-5754", score: 5.6,
      title: "Meltdown – Rogue Data Cache Load",
      description: "Systems with microprocessors utilizing speculative execution and indirect branch prediction may allow unauthorized disclosure of information to an attacker with local user access via a side-channel analysis of the data cache. Allows user-mode code to read kernel memory.",
      affected_products: "Intel processors (most Core i3/i5/i7/i9 since ~2010); some ARM Cortex-A processors. AMD processors not believed to be affected by the original Meltdown variant.",
      remediation: "Apply Kernel Page-Table Isolation (KPTI) patches from OS vendors. Update hypervisors. Apply CPU microcode updates. Monitor for performance regressions in I/O-intensive workloads.",
      references: "https://nvd.nist.gov/vuln/detail/CVE-2017-5754, https://meltdownattack.com/",
      statement: "MEDIUM severity (per NVD). Allows reading arbitrary kernel memory from user space, exposing passwords, encryption keys, and other sensitive data. KPTI patches carry measurable overhead. Patch immediately.",
      reporter: "NVD / Google Project Zero / Graz University of Technology" },

    { cve_id: "CVE-2023-44487", score: 7.5,
      title: "HTTP/2 Rapid Reset – DDoS Amplification",
      description: "The HTTP/2 protocol allows a denial of service (server resource consumption) because request cancellation can reset many streams quickly, as exploited in the wild in August through October 2023.",
      affected_products: "All HTTP/2 server implementations including nginx, Apache httpd, Microsoft IIS, Go net/http, Node.js, Tomcat, and cloud load balancers.",
      remediation: "Apply patches from web server and runtime vendors. Implement connection-level rate limiting. Set SETTINGS_MAX_CONCURRENT_STREAMS to a low value (e.g., 100). Use a CDN or DDoS mitigation service.",
      references: "https://nvd.nist.gov/vuln/detail/CVE-2023-44487, https://cloud.google.com/blog/products/identity-security/how-it-works-the-novel-http2-rapid-reset-ddos-attack",
      statement: "HIGH severity. Exploited to generate record-breaking DDoS attacks exceeding 398 million requests/second. Requires no authentication. Apply HTTP/2 server patches and rate-limit RST_STREAM frames urgently.",
      reporter: "NVD / Google / Cloudflare / Amazon" },

    { cve_id: "CVE-2023-23397", score: 9.8,
      title: "Microsoft Outlook NTLM Hash Leak – Zero-Click",
      description: "Microsoft Outlook elevation of privilege vulnerability. A specially crafted email with a UNC path triggers an NTLM authentication request to an attacker-controlled server when Outlook renders the reminder, with no user interaction required beyond receiving the email.",
      affected_products: "Microsoft Outlook for Windows (all supported versions). Exchange Online users are partially protected by email filtering but on-premises delivery may not strip the malicious header.",
      remediation: "Apply March 2023 Patch Tuesday update (KB5002333 or equivalent). Add users to the Protected Users AD group. Block outbound SMB (TCP 445) at the perimeter. Use the Microsoft script to check for exploitation IOCs.",
      references: "https://nvd.nist.gov/vuln/detail/CVE-2023-23397, https://msrc.microsoft.com/update-guide/vulnerability/CVE-2023-23397",
      statement: "CRITICAL severity. Zero-click, pre-interaction exploit. Attributed to Russian APT28 (Fancy Bear). Harvested NTLM hashes can be used for Pass-the-Hash or relayed attacks. Patch Outlook immediately.",
      reporter: "NVD / Microsoft MSRC / CERT-UA" },

    { cve_id: "CVE-2024-3094", score: 10.0,
      title: "XZ Utils Backdoor – Supply Chain Attack",
      description: "Malicious code was discovered in the upstream tarballs of xz, starting with version 5.6.0. The backdoored liblzma allows an attacker to break RSA key validation in sshd, enabling unauthorized remote access on affected systems.",
      affected_products: "XZ Utils versions 5.6.0 and 5.6.1 as distributed in rolling-release Linux distributions (Fedora Rawhide, Debian unstable/experimental, openSUSE Tumbleweed, Arch Linux, Kali Linux, Gentoo) between Feb-Mar 2024.",
      remediation: "Downgrade xz-utils to version 5.4.x immediately. Verify binary integrity against trusted repositories. Rotate SSH host keys and audit SSH access logs on potentially affected systems.",
      references: "https://nvd.nist.gov/vuln/detail/CVE-2024-3094, https://www.openwall.com/lists/oss-security/2024/03/29/4",
      statement: "CRITICAL severity. Supply chain attack discovered fortuitously by a Microsoft engineer via anomalous CPU usage. Multi-year, nation-state-level sophistication. Only stable distributions were not affected.",
      reporter: "NVD / Andres Freund / Red Hat Security" }
  ].freeze

  EXTRA_NOTES = [
    "Coordinated disclosure followed. Vendor responded within 90 days.",
    "Public exploit code available on GitHub and Exploit-DB. Treat as actively exploited.",
    "No known public exploits at time of advisory publication.",
    "Exploitation requires local access; network-based exploitation not demonstrated.",
    "CISA Added to Known Exploited Vulnerabilities catalog. Federal agencies have 72 hours to remediate.",
    "Bug bounty awarded: $10,000 via HackerOne.",
    "Discovered during routine internal red team exercise.",
    "Vendor disputed severity; NVD score reflects independent analysis.",
    "Workaround available; full patch expected in next quarterly release.",
    "CVSS environmental score may differ based on deployment configuration.",
    "Actively targeted by ransomware affiliate groups as of last threat intel update.",
    "Proof-of-concept released 7 days after patch; exploitation observed within 24 hours of PoC.",
    "No authentication required for exploitation; internet-exposed instances at highest risk.",
    "Dual use: same technique used in legitimate penetration testing tooling.",
    "Fix introduced a regression; patched again in the following minor release."
  ].freeze

  module_function

  def create_table(db)
    db.run("DROP TABLE IF EXISTS security_statements")
    db.run(<<~SQL)
      CREATE TABLE security_statements (
        id                BIGSERIAL       PRIMARY KEY,
        cve_id            VARCHAR(30)     NOT NULL,
        score             NUMERIC(3,1)    NOT NULL CHECK (score >= 0.0 AND score <= 10.0),
        status            VARCHAR(12)     NOT NULL CHECK (status IN ('affected', 'non_affected')),
        statement         TEXT            NOT NULL,
        title             VARCHAR(512),
        description       TEXT,
        affected_products TEXT,
        remediation       TEXT,
        "references"      TEXT,
        published_at      TIMESTAMP       NOT NULL DEFAULT NOW(),
        updated_at        TIMESTAMP       NOT NULL DEFAULT NOW(),
        reporter          VARCHAR(255),
        notes             TEXT
      )
    SQL
    db.run("CREATE INDEX idx_sec_cve_id       ON security_statements (cve_id)")
    db.run("CREATE INDEX idx_sec_status       ON security_statements (status)")
    db.run("CREATE INDEX idx_sec_score        ON security_statements (score)")
    db.run("CREATE INDEX idx_sec_published_at ON security_statements (published_at)")
  end

  def populate(db, num_rows)
    rng      = Random.new(42)
    statuses = %w[affected non_affected]

    puts "\nInserting #{num_rows} rows into security_statements…"
    start = Time.now

    (0...num_rows).each_slice(BATCH_SIZE) do |slice|
      rows = slice.map do
        cve   = CVE_POOL[rng.rand(CVE_POOL.size)]
        raw   = cve[:score] + (rng.rand - 0.5) * 2.0
        score = [[raw, 0.0].max, 10.0].min.round(1)
        {
          cve_id:            cve[:cve_id],
          score:             score,
          status:            statuses[rng.rand(2)],
          statement:         cve[:statement],
          title:             cve[:title],
          description:       cve[:description],
          affected_products: cve[:affected_products],
          remediation:       cve[:remediation],
          references:        cve[:references],
          reporter:          cve[:reporter],
          notes:             EXTRA_NOTES[rng.rand(EXTRA_NOTES.size)]
        }
      end
      db[:security_statements].multi_insert(rows)
    end

    puts "Done: #{num_rows} rows in #{'%.1f' % (Time.now - start)}s"
  end

  def drop_table(db)
    db.run("DROP TABLE IF EXISTS security_statements")
  end
end
