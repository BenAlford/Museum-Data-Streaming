output "instance_hostname" {
  description = "Endpoint of the RDS instance (reachable only from the bastion)."
  value       = aws_db_instance.default.endpoint
}

output "bastion_public_ip" {
  description = "Public IP of the bastion EC2 instance."
  value       = aws_instance.bastion.public_ip
}
