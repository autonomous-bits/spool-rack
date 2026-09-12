# Security Policy

## Reporting a vulnerability

Please report suspected vulnerabilities privately through
[GitHub Security Advisories](https://github.com/autonomous-bits/spool-rack/security/advisories/new).
Do not open a public issue, discussion, or pull request.

1. Open the link above and select **Report a vulnerability**.
2. Include a clear description of the issue, affected versions or commits, steps to reproduce,
   impact, and a proof of concept where safe to provide one.
3. Submit the report. The report becomes a draft repository security advisory that is visible
   only to repository maintainers and invited collaborators.
4. Use the advisory's private discussion to answer questions and coordinate disclosure. Please
   give maintainers reasonable time to investigate and release a fix before publicly disclosing
   the vulnerability.

## Private remediation and disclosure

Repository maintainers use the draft advisory to coordinate remediation before users are exposed
to the vulnerability:

1. Review and accept the private report in **Security** > **Advisories**, then communicate with
   the reporter in the advisory.
2. Invite only the people needed to investigate and fix the issue. If code changes must remain
   private, create the advisory's temporary private fork and develop the fix there.
3. Test the fix, release a patched version, and record the affected and patched versions in the
   advisory.
4. Complete the advisory with a summary, impact, severity, CWE where applicable, CVSS details,
   credits, and mitigation or upgrade guidance. Request a CVE from GitHub when appropriate.
5. Publish the advisory after the fix is available. Publishing discloses the vulnerability to the
   community and gives users the information needed to assess impact and upgrade.

We will credit reporters unless they prefer to remain anonymous.
